"""Nodo de almacenamiento gRPC.

Guarda archivos en su propia carpeta y los sirve por streaming.
Al arrancar intenta registrarse en el coordinador.

Uso:
    python nodo.py --id nodo1 --puerto 50051
"""
import argparse
import hashlib
import json
import logging
import os
import threading
from concurrent import futures
from datetime import datetime

import grpc

import almacenamiento_pb2 as pb
import almacenamiento_pb2_grpc as pb_grpc
import config
from config import TAMANO_FRAGMENTO


class NodoServicer(pb_grpc.NodoAlmacenamientoServicer):
    """Implementa los métodos del servicio NodoAlmacenamiento."""

    def __init__(self, nodo_id, carpeta):
        self.nodo_id = nodo_id
        self.carpeta = carpeta
        self.ruta_meta = os.path.join(carpeta, "_metadatos.json")
        self.candado = threading.Lock()  # protege el índice ante hilos simultáneos
        os.makedirs(carpeta, exist_ok=True)
        self.metadatos = self._cargar_metadatos()

    # ---------- utilidades ----------
    def _cargar_metadatos(self):
        if os.path.exists(self.ruta_meta):
            with open(self.ruta_meta, encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _guardar_metadatos(self):
        with open(self.ruta_meta, "w", encoding="utf-8") as f:
            json.dump(self.metadatos, f, indent=2, ensure_ascii=False)

    @staticmethod
    def _nombre_seguro(nombre):
        return os.path.basename(nombre)  # evita rutas como ../../archivo

    # ---------- métodos remotos ----------
    def Ping(self, request, context):
        logging.info("Ping recibido desde %s", request.origen)
        return pb.RespuestaPing(nodo_id=self.nodo_id,
                                archivos_guardados=len(self.metadatos))

    def SubirArchivo(self, request_iterator, context):
        nombre, total, fragmentos = None, 0, 0
        sha = hashlib.sha256()
        archivo = None
        try:
            for fragmento in request_iterator:
                if archivo is None:
                    nombre = self._nombre_seguro(fragmento.nombre)
                    if not nombre:
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                      "El primer fragmento debe traer el nombre")
                    archivo = open(os.path.join(self.carpeta, nombre + ".tmp"), "wb")
                archivo.write(fragmento.datos)
                sha.update(fragmento.datos)
                total += len(fragmento.datos)
                fragmentos += 1
        finally:
            if archivo:
                archivo.close()

        if nombre is None:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "No se recibieron fragmentos")

        ruta = os.path.join(self.carpeta, nombre)
        os.replace(ruta + ".tmp", ruta)  # el archivo solo aparece si llegó completo
        with self.candado:
            self.metadatos[nombre] = {
                "tamano_bytes": total,
                "hash_sha256": sha.hexdigest(),
                "fecha_subida": datetime.now().isoformat(timespec="seconds"),
            }
            self._guardar_metadatos()
        logging.info("Archivo '%s' guardado: %d bytes en %d fragmentos",
                     nombre, total, fragmentos)
        return pb.ResultadoSubida(ok=True, nodo_id=self.nodo_id, tamano_bytes=total,
                                  fragmentos=fragmentos, hash_sha256=sha.hexdigest())

    def _fragmentos(self, nombre):
        """Lee un archivo guardado y lo entrega en fragmentos de 64 KB."""
        with open(os.path.join(self.carpeta, nombre), "rb") as f:
            indice = 0
            while bloque := f.read(TAMANO_FRAGMENTO):
                yield pb.Fragmento(nombre=nombre, indice=indice, datos=bloque)
                indice += 1

    def _existe(self, nombre):
        return nombre in self.metadatos and os.path.exists(os.path.join(self.carpeta, nombre))

    def DescargarArchivo(self, request, context):
        nombre = self._nombre_seguro(request.nombre)
        if not self._existe(nombre):
            context.abort(grpc.StatusCode.NOT_FOUND,
                          f"El archivo '{nombre}' no existe en {self.nodo_id}")
        logging.info("Enviando '%s'", nombre)
        yield from self._fragmentos(nombre)

    def ReplicarHacia(self, request, context):
        """Orden del coordinador: enviar una copia de un archivo propio a otro nodo.
        Aquí este nodo actúa como CLIENTE gRPC del nodo destino (plano de datos):
        los bytes viajan de nodo a nodo sin pasar por el coordinador."""
        nombre = self._nombre_seguro(request.nombre)
        if not self._existe(nombre):
            context.abort(grpc.StatusCode.NOT_FOUND,
                          f"El archivo '{nombre}' no existe en {self.nodo_id}")
        logging.info("Replicando '%s' hacia %s", nombre, request.destino)
        try:
            with grpc.insecure_channel(request.destino) as canal:
                resultado = pb_grpc.NodoAlmacenamientoStub(canal).SubirArchivo(
                    self._fragmentos(nombre), timeout=config.PLAZO_ARCHIVO)
        except grpc.RpcError as e:
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          f"No se pudo copiar '{nombre}' a {request.destino}: {e.code().name}")
        logging.info("'%s' copiado a %s (%d bytes)", nombre, resultado.nodo_id,
                     resultado.tamano_bytes)
        return resultado

    def ConsultarMetadatos(self, request, context):
        nombre = self._nombre_seguro(request.nombre)
        meta = self.metadatos.get(nombre)
        if meta is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          f"El archivo '{nombre}' no existe en {self.nodo_id}")
        return pb.Metadatos(nombre=nombre, nodo_id=self.nodo_id, **meta)

    def EliminarArchivo(self, request, context):
        nombre = self._nombre_seguro(request.nombre)
        with self.candado:
            if nombre not in self.metadatos:
                context.abort(grpc.StatusCode.NOT_FOUND,
                              f"El archivo '{nombre}' no existe en {self.nodo_id}")
            ruta = os.path.join(self.carpeta, nombre)
            if os.path.exists(ruta):
                os.remove(ruta)
            del self.metadatos[nombre]
            self._guardar_metadatos()
        logging.info("Archivo '%s' eliminado", nombre)
        return pb.RespuestaRegistro(ok=True, mensaje=f"'{nombre}' eliminado de {self.nodo_id}")

def registrar_en_coordinador(coordinador, nodo_id, direccion):
    """Avisa al coordinador que este nodo está disponible."""
    try:
        with grpc.insecure_channel(coordinador) as canal:
            stub = pb_grpc.CoordinadorStub(canal)
            r = stub.RegistrarNodo(pb.InfoNodo(nodo_id=nodo_id, direccion=direccion),
                                   timeout=config.PLAZO)
            logging.info("Registrado en el coordinador: %s", r.mensaje)
    except grpc.RpcError as e:
        logging.warning("No se pudo registrar en el coordinador %s (%s). "
                        "El nodo funciona igual de forma independiente.",
                        coordinador, e.code().name)


def main():
    parser = argparse.ArgumentParser(description="Nodo de almacenamiento gRPC")
    parser.add_argument("--id", default="nodo1", help="identificador del nodo")
    parser.add_argument("--puerto", type=int, default=None,
                        help="puerto del nodo (por defecto según su id: nodo2 -> 50052)")
    parser.add_argument("--host", default=config.IP_PROPIA,
                        help="dirección con la que otros contactan a este nodo")
    parser.add_argument("--coordinador", default=config.COORDINADOR,
                        help="host:puerto del coordinador")
    args = parser.parse_args()
    if args.puerto is None:
        args.puerto = config.puerto_de_nodo(args.id)

    logging.basicConfig(level=logging.INFO,
                        format=f"%(asctime)s [{args.id}] %(message)s")
    carpeta = f"datos_{args.id}"

    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_NodoAlmacenamientoServicer_to_server(
        NodoServicer(args.id, carpeta), servidor)
    servidor.add_insecure_port(f"[::]:{args.puerto}")
    servidor.start()
    logging.info("Nodo escuchando en el puerto %d (datos en %s)", args.puerto, carpeta)

    registrar_en_coordinador(args.coordinador, args.id, f"{args.host}:{args.puerto}")
    servidor.wait_for_termination()

if __name__ == "__main__":
    main()