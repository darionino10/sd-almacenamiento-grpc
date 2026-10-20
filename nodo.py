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

TAMANO_FRAGMENTO = 64 * 1024  # 64 KB por fragmento


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

    def DescargarArchivo(self, request, context):
        nombre = self._nombre_seguro(request.nombre)
        ruta = os.path.join(self.carpeta, nombre)
        if nombre not in self.metadatos or not os.path.exists(ruta):
            context.abort(grpc.StatusCode.NOT_FOUND,
                          f"El archivo '{nombre}' no existe en {self.nodo_id}")
        logging.info("Enviando '%s'", nombre)
        with open(ruta, "rb") as f:
            indice = 0
            while bloque := f.read(TAMANO_FRAGMENTO):
                yield pb.Fragmento(nombre=nombre, indice=indice, datos=bloque)
                indice += 1

    def ConsultarMetadatos(self, request, context):
        nombre = self._nombre_seguro(request.nombre)
        meta = self.metadatos.get(nombre)
        if meta is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          f"El archivo '{nombre}' no existe en {self.nodo_id}")
        return pb.Metadatos(nombre=nombre, nodo_id=self.nodo_id, **meta)


def registrar_en_coordinador(coordinador, nodo_id, direccion):
    """Avisa al coordinador que este nodo está disponible."""
    try:
        with grpc.insecure_channel(coordinador) as canal:
            stub = pb_grpc.CoordinadorStub(canal)
            r = stub.RegistrarNodo(pb.InfoNodo(nodo_id=nodo_id, direccion=direccion),
                                   timeout=3)
            logging.info("Registrado en el coordinador: %s", r.mensaje)
    except grpc.RpcError as e:
        logging.warning("No se pudo registrar en el coordinador %s (%s). "
                        "El nodo funciona igual de forma independiente.",
                        coordinador, e.code().name)


def main():
    parser = argparse.ArgumentParser(description="Nodo de almacenamiento gRPC")
    parser.add_argument("--id", default="nodo1", help="identificador del nodo")
    parser.add_argument("--puerto", type=int, default=50051)
    parser.add_argument("--host", default="localhost",
                        help="dirección con la que otros contactan a este nodo")
    parser.add_argument("--coordinador", default="localhost:50050",
                        help="host:puerto del coordinador")
    args = parser.parse_args()

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