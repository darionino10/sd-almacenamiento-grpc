"""Cliente de consola del sistema de almacenamiento distribuido.

Consulta al coordinador dónde guardar o buscar cada archivo y luego
habla directamente con los nodos por gRPC.

Ejemplos:
    python cliente.py subir ejemplo.txt
    python cliente.py listar
    python cliente.py info ejemplo.txt
    python cliente.py descargar ejemplo.txt
    python cliente.py ping localhost:50051
"""
import argparse
import hashlib
import os
import sys
import time

import grpc

import almacenamiento_pb2 as pb
import almacenamiento_pb2_grpc as pb_grpc
import config
from config import PLAZO, PLAZO_ARCHIVO, TAMANO_FRAGMENTO

# Quién hace las acciones desde la consola (se envía al coordinador en los metadatos gRPC)
METADATOS = [("usuario", "consola")]

def generar_fragmentos(ruta):
    """Lee el archivo por bloques y produce mensajes Fragmento (streaming)."""
    nombre = os.path.basename(ruta)
    with open(ruta, "rb") as f:
        indice = 0
        while bloque := f.read(TAMANO_FRAGMENTO):
            yield pb.Fragmento(nombre=nombre, indice=indice, datos=bloque)
            indice += 1


def hash_local(ruta):
    sha = hashlib.sha256()
    with open(ruta, "rb") as f:
        while bloque := f.read(TAMANO_FRAGMENTO):
            sha.update(bloque)
    return sha.hexdigest()


def ubicar(coordinador, nombre, operacion, accion=""):
    """Pregunta al coordinador qué nodos usar."""
    metadatos = METADATOS + ([("accion", accion)] if accion else [])
    with grpc.insecure_channel(coordinador) as canal:
        stub = pb_grpc.CoordinadorStub(canal)
        return stub.UbicarArchivo(
            pb.SolicitudUbicacion(nombre=nombre, operacion=operacion), timeout=PLAZO,
            metadata=metadatos)

# ---------------- comandos ----------------
def subir(args):
    if not os.path.exists(args.archivo):
        sys.exit(f"No existe el archivo local '{args.archivo}'")
    nombre = os.path.basename(args.archivo)
    esperado = hash_local(args.archivo)
    tamano = os.path.getsize(args.archivo)
    ubicacion = ubicar(args.coordinador, nombre, pb.SUBIR)
    print(f"Coordinador asignó: {', '.join(n.nodo_id for n in ubicacion.nodos)}")

    nodos_ok = []
    for nodo in ubicacion.nodos:
        inicio = time.perf_counter()
        try:
            with grpc.insecure_channel(nodo.direccion) as canal:
                r = pb_grpc.NodoAlmacenamientoStub(canal).SubirArchivo(
                    generar_fragmentos(args.archivo), timeout=PLAZO_ARCHIVO)
            ms = (time.perf_counter() - inicio) * 1000
            integro = r.hash_sha256 == esperado
            print(f"  -> {r.nodo_id}: {r.tamano_bytes} bytes, {r.fragmentos} fragmento(s), "
                  f"{ms:.1f} ms, hash {'OK' if integro else 'NO COINCIDE'}")
            if integro:
                nodos_ok.append(r.nodo_id)
        except grpc.RpcError as e:
            print(f"  -> {nodo.nodo_id}: ERROR [{e.code().name}] {e.details()}")

    if not nodos_ok:
        sys.exit(f"La subida de '{nombre}' falló en todos los nodos; no se registra")

    # Solo ahora el coordinador registra el archivo, con los nodos que sí lo recibieron.
    with grpc.insecure_channel(args.coordinador) as canal:
        r = pb_grpc.CoordinadorStub(canal).ConfirmarSubida(
            pb.ConfirmacionSubida(nombre=nombre, nodos_ok=nodos_ok,
                                  hash_sha256=esperado, tamano_bytes=tamano),
            timeout=PLAZO, metadata=METADATOS)
    print(f"Confirmado por el coordinador: {r.mensaje}")

def descargar(args):
    nombre = os.path.basename(args.archivo)
    destino = args.destino or f"descargado_{nombre}"
    ubicacion = ubicar(args.coordinador, nombre, pb.DESCARGAR, accion="descarga")
    print(f"Réplicas según el coordinador: {', '.join(n.nodo_id for n in ubicacion.nodos)}")

    for nodo in ubicacion.nodos:
        print(f"Intentando descargar desde {nodo.nodo_id} ({nodo.direccion})...")
        inicio = time.perf_counter()
        sha = hashlib.sha256()
        try:
            n = 0
            with grpc.insecure_channel(nodo.direccion) as canal, \
                    open(destino + ".parcial", "wb") as f:
                stub = pb_grpc.NodoAlmacenamientoStub(canal)
                for fragmento in stub.DescargarArchivo(pb.SolicitudArchivo(nombre=nombre),
                                                       timeout=PLAZO_ARCHIVO):
                    f.write(fragmento.datos)
                    sha.update(fragmento.datos)
                    n += 1
            os.replace(destino + ".parcial", destino)
            ms = (time.perf_counter() - inicio) * 1000
            print(f"  Descargado desde {nodo.nodo_id} en {destino} "
                  f"({n} fragmento(s), {ms:.1f} ms)")
            print(f"  SHA-256: {sha.hexdigest()}")
            return
        except grpc.RpcError as e:
            print(f"  Falló {nodo.nodo_id}: [{e.code().name}] -> se prueba la siguiente réplica")
            if os.path.exists(destino + ".parcial"):
                os.remove(destino + ".parcial")
    sys.exit(f"No se pudo descargar '{nombre}': ninguna réplica respondió")


def info(args):
    nombre = os.path.basename(args.archivo)
    ubicacion = ubicar(args.coordinador, nombre, pb.DESCARGAR)
    for nodo in ubicacion.nodos:
        try:
            with grpc.insecure_channel(nodo.direccion) as canal:
                m = pb_grpc.NodoAlmacenamientoStub(canal).ConsultarMetadatos(
                    pb.SolicitudArchivo(nombre=nombre), timeout=PLAZO)
            print(f"{m.nombre} en {m.nodo_id}: {m.tamano_bytes} bytes, subido {m.fecha_subida}")
            print(f"SHA-256: {m.hash_sha256}")
        except grpc.RpcError as e:
            print(f"{nodo.nodo_id}: ERROR [{e.code().name}] {e.details()}")

def eliminar(args):
    nombre = os.path.basename(args.archivo)
    with grpc.insecure_channel(args.coordinador) as canal:
        r = pb_grpc.CoordinadorStub(canal).EliminarArchivo(
            pb.SolicitudArchivo(nombre=nombre), timeout=PLAZO * 3, metadata=METADATOS)
    print(f"'{r.nombre}' eliminado de: {', '.join(r.nodos_eliminados) or 'ningún nodo'}")
    if r.nodos_fallidos:
        print(f"  No se pudo borrar en {', '.join(r.nodos_fallidos)} (nodo no disponible)")

def listar(args):
    with grpc.insecure_channel(args.coordinador) as canal:
        lista = pb_grpc.CoordinadorStub(canal).ListarArchivos(pb.Vacio(), timeout=PLAZO)
    if not lista.archivos:
        print("No hay archivos registrados")
    for a in lista.archivos:
        print(f"{a.nombre:30} réplicas: {', '.join(a.nodos)}")


def ping(args):
    with grpc.insecure_channel(args.nodo) as canal:
        r = pb_grpc.NodoAlmacenamientoStub(canal).Ping(
            pb.SolicitudPing(origen="cliente"), timeout=PLAZO)
    print(f"Respuesta de {r.nodo_id}: {r.archivos_guardados} archivo(s) guardado(s)")


def main():
    parser = argparse.ArgumentParser(description="Cliente de almacenamiento distribuido")
    parser.add_argument("--coordinador", default=config.COORDINADOR,
                        help="host:puerto del coordinador")
    sub = parser.add_subparsers(dest="comando", required=True)
    p = sub.add_parser("subir"); p.add_argument("archivo")
    p = sub.add_parser("descargar"); p.add_argument("archivo"); p.add_argument("--destino")
    p = sub.add_parser("info"); p.add_argument("archivo")
    p = sub.add_parser("eliminar"); p.add_argument("archivo")
    sub.add_parser("listar")
    p = sub.add_parser("ping"); p.add_argument("nodo", help="host:puerto del nodo")
    args = parser.parse_args()

    acciones = {"subir": subir, "descargar": descargar, "info": info,
                "eliminar": eliminar, "listar": listar, "ping": ping}
    try:
        acciones[args.comando](args)
    except grpc.RpcError as e:
        # Errores del coordinador o del nodo: se muestra el código de estado gRPC
        sys.exit(f"ERROR gRPC [{e.code().name}]: {e.details()}")

if __name__ == "__main__":
    main()