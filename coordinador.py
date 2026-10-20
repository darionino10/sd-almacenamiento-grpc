"""Coordinador del sistema de almacenamiento distribuido.

- Registra los nodos que se anuncian al arrancar.
- Comprueba cada pocos segundos qué nodos siguen vivos (Ping).
- Decide en qué dos nodos se guarda cada archivo (replicación).
- Indica al cliente desde qué nodos descargar, poniendo primero los vivos.

Uso:
    python coordinador.py --puerto 50050
"""
import argparse
import json
import logging
import os
import threading
import time
from concurrent import futures

import grpc

import almacenamiento_pb2 as pb
import almacenamiento_pb2_grpc as pb_grpc

REPLICAS = 2               # cada archivo se guarda en dos nodos
INTERVALO_SALUD = 5        # segundos entre comprobaciones de los nodos
CARPETA = "datos_coordinador"
RUTA_INDICE = os.path.join(CARPETA, "indice.json")


class CoordinadorServicer(pb_grpc.CoordinadorServicer):

    def __init__(self):
        self.candado = threading.Lock()
        self.nodos = {}      # nodo_id -> {"direccion": str, "vivo": bool}
        self.archivos = {}   # nombre -> [nodo_id, nodo_id]
        os.makedirs(CARPETA, exist_ok=True)
        self._cargar_indice()

    # ---------- persistencia del índice ----------
    def _cargar_indice(self):
        if os.path.exists(RUTA_INDICE):
            with open(RUTA_INDICE, encoding="utf-8") as f:
                datos = json.load(f)
            self.archivos = datos.get("archivos", {})
            for nodo_id, direccion in datos.get("nodos", {}).items():
                self.nodos[nodo_id] = {"direccion": direccion, "vivo": False}

    def _guardar_indice(self):
        datos = {"nodos": {i: n["direccion"] for i, n in self.nodos.items()},
                 "archivos": self.archivos}
        with open(RUTA_INDICE, "w", encoding="utf-8") as f:
            json.dump(datos, f, indent=2, ensure_ascii=False)

    def _info(self, nodo_id):
        return pb.InfoNodo(nodo_id=nodo_id, direccion=self.nodos[nodo_id]["direccion"])

    # ---------- métodos remotos ----------
    def RegistrarNodo(self, request, context):
        with self.candado:
            anterior = self.nodos.get(request.nodo_id)
            self.nodos[request.nodo_id] = {"direccion": request.direccion, "vivo": True}
            self._guardar_indice()
        if anterior is None:
            logging.info("Nodo registrado: %s en %s", request.nodo_id, request.direccion)
        elif not anterior["vivo"]:
            logging.warning("Nodo %s VOLVIÓ a estar disponible (se registró de nuevo)",
                            request.nodo_id)
        else:
            logging.info("Nodo %s se registró de nuevo", request.nodo_id)
        return pb.RespuestaRegistro(ok=True, mensaje=f"{request.nodo_id} registrado")

    def UbicarArchivo(self, request, context):
        nombre = os.path.basename(request.nombre)
        with self.candado:
            vivos = [i for i, n in self.nodos.items() if n["vivo"]]

            if request.operacion == pb.SUBIR:
                if not vivos:
                    context.abort(grpc.StatusCode.UNAVAILABLE,
                                  "No hay nodos de almacenamiento disponibles")
                # Se eligen los nodos vivos con menos archivos (reparto de carga).
                carga = {i: sum(i in r for r in self.archivos.values()) for i in vivos}
                elegidos = sorted(vivos, key=lambda i: carga[i])[:REPLICAS]
                if len(elegidos) < REPLICAS:
                    logging.warning("Solo hay %d nodo(s) vivo(s): '%s' tendrá menos réplicas",
                                    len(elegidos), nombre)
                self.archivos[nombre] = elegidos
                self._guardar_indice()
                logging.info("Subida de '%s' asignada a %s", nombre, elegidos)
                return pb.Ubicacion(nombre=nombre, nodos=[self._info(i) for i in elegidos])

            # DESCARGAR
            if nombre not in self.archivos:
                context.abort(grpc.StatusCode.NOT_FOUND,
                              f"El coordinador no conoce el archivo '{nombre}'")
            replicas = self.archivos[nombre]
            # Primero los nodos vivos; los caídos quedan al final como último intento.
            ordenados = sorted(replicas, key=lambda i: not self.nodos.get(i, {}).get("vivo"))
            logging.info("Descarga de '%s': réplicas %s", nombre, ordenados)
            return pb.Ubicacion(nombre=nombre,
                                nodos=[self._info(i) for i in ordenados if i in self.nodos])

    def ListarArchivos(self, request, context):
        with self.candado:
            entradas = [pb.EntradaArchivo(nombre=n, nodos=r) for n, r in self.archivos.items()]
        return pb.ListaArchivos(archivos=entradas)

    # ---------- comprobación de salud ----------
    def vigilar_nodos(self):
        """Hace Ping a cada nodo periódicamente y actualiza si está vivo."""
        while True:
            with self.candado:
                lista = list(self.nodos.items())
            for nodo_id, nodo in lista:
                try:
                    with grpc.insecure_channel(nodo["direccion"]) as canal:
                        pb_grpc.NodoAlmacenamientoStub(canal).Ping(
                            pb.SolicitudPing(origen="coordinador"), timeout=2)
                    vivo = True
                except grpc.RpcError:
                    vivo = False
                with self.candado:
                    if nodo_id in self.nodos and self.nodos[nodo_id]["vivo"] != vivo:
                        self.nodos[nodo_id]["vivo"] = vivo
                        estado = "VOLVIÓ a estar disponible" if vivo else "NO RESPONDE (caído)"
                        logging.warning("Nodo %s %s", nodo_id, estado)
            time.sleep(INTERVALO_SALUD)


def main():
    parser = argparse.ArgumentParser(description="Coordinador gRPC")
    parser.add_argument("--puerto", type=int, default=50050)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [coordinador] %(message)s")

    servicer = CoordinadorServicer()
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_CoordinadorServicer_to_server(servicer, servidor)
    servidor.add_insecure_port(f"[::]:{args.puerto}")
    servidor.start()
    logging.info("Coordinador escuchando en el puerto %d", args.puerto)

    threading.Thread(target=servicer.vigilar_nodos, daemon=True).start()
    servidor.wait_for_termination()


if __name__ == "__main__":
    main()