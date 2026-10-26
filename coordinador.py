"""Coordinador del sistema de almacenamiento distribuido.

- Registra los nodos que se anuncian al arrancar.
- Comprueba cada pocos segundos qué nodos siguen vivos (Ping).
- Decide en qué dos nodos se guarda cada archivo (replicación).
- Indica al cliente desde qué nodos descargar, poniendo primero los vivos.
- Guarda el índice y la bitácora de eventos en SQLite (indice.py).

Uso:
    python coordinador.py --puerto 50050
"""
import argparse
import logging
import os
import threading
import time
from concurrent import futures

import grpc

import almacenamiento_pb2 as pb
import almacenamiento_pb2_grpc as pb_grpc
import config
from config import INTERVALO_SALUD, PLAZO_PING, REPLICAS
from indice import Indice

CARPETA = "datos_coordinador"
RUTA_BD = os.path.join(CARPETA, "coordinador.db")
RUTA_JSON_ANTERIOR = os.path.join(CARPETA, "indice.json")


class CoordinadorServicer(pb_grpc.CoordinadorServicer):

    def __init__(self):
        self.candado = threading.Lock()
        self.indice = Indice(RUTA_BD)
        migrados = self.indice.migrar_json(RUTA_JSON_ANTERIOR)
        if migrados:
            self._evento("migracion", f"Se importaron {migrados} archivo(s) desde indice.json")
        # El estado vivo/caído solo vive en memoria: al arrancar, todos empiezan como caídos
        # hasta que se registran o responden al ping.
        self.nodos = {i: {"direccion": d, "vivo": False} for i, d in self.indice.nodos().items()}
        self.pendientes = {}   # nombre -> nodos asignados que aún no confirman la subida
        self.vistos = set()    # nodos que ya respondieron en esta sesión del coordinador
        self._evento("coordinador_iniciado",
                     f"Coordinador iniciado con {len(self.nodos)} nodo(s) conocido(s)")

    def _evento(self, tipo, detalle, nivel=logging.INFO):
        """Escribe el evento en la bitácora (SQLite) y en el registro de la terminal."""
        self.indice.evento(tipo, detalle)
        logging.log(nivel, detalle)

    def _info(self, nodo_id):
        return pb.InfoNodo(nodo_id=nodo_id, direccion=self.nodos[nodo_id]["direccion"])

    # ---------- métodos remotos ----------
    def RegistrarNodo(self, request, context):
        with self.candado:
            anterior = self.nodos.get(request.nodo_id)
            visto = request.nodo_id in self.vistos
            self.nodos[request.nodo_id] = {"direccion": request.direccion, "vivo": True}
            self.vistos.add(request.nodo_id)
            self.indice.guardar_nodo(request.nodo_id, request.direccion)
        if anterior is None:
            self._evento("nodo_registrado",
                         f"Nodo registrado: {request.nodo_id} en {request.direccion}")
        elif not visto:
            # Ya era conocido, pero es su primera conexión desde que arrancó el coordinador
            self._evento("nodo_conectado", f"Nodo {request.nodo_id} conectado en {request.direccion}")
        elif not anterior["vivo"]:
            self._evento("nodo_volvio",
                         f"Nodo {request.nodo_id} VOLVIÓ a estar disponible (se registró de nuevo)",
                         logging.WARNING)
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
                carga = self.indice.carga()
                elegidos = sorted(vivos, key=lambda i: carga.get(i, 0))[:REPLICAS]
                if len(elegidos) < REPLICAS:
                    logging.warning("Solo hay %d nodo(s) vivo(s): '%s' tendrá menos réplicas",
                                    len(elegidos), nombre)
                # Aún no se registra: se espera a que el cliente confirme la subida.
                self.pendientes[nombre] = elegidos
                logging.info("Subida de '%s' asignada a %s (pendiente de confirmar)",
                             nombre, elegidos)
                return pb.Ubicacion(nombre=nombre, nodos=[self._info(i) for i in elegidos])

            # DESCARGAR
            replicas = self.indice.replicas(nombre)
            if replicas is None:
                context.abort(grpc.StatusCode.NOT_FOUND,
                              f"El coordinador no conoce el archivo '{nombre}'")
            # Primero los nodos vivos; los caídos quedan al final como último intento.
            ordenados = sorted(replicas, key=lambda i: not self.nodos.get(i, {}).get("vivo"))
            logging.info("Descarga de '%s': réplicas %s", nombre, ordenados)
            return pb.Ubicacion(nombre=nombre,
                                nodos=[self._info(i) for i in ordenados if i in self.nodos])

    def ConfirmarSubida(self, request, context):
        nombre = os.path.basename(request.nombre)
        with self.candado:
            asignados = self.pendientes.pop(nombre, None)
            if asignados is None:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                              f"No hay una subida pendiente para '{nombre}'")
            # Solo cuentan los nodos que el coordinador asignó y que el cliente confirma.
            confirmados = [i for i in request.nodos_ok if i in asignados]
            if not confirmados:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                              f"La subida de '{nombre}' no se completó en ningún nodo")
            self.indice.registrar_archivo(nombre, confirmados, request.tamano_bytes,
                                          request.hash_sha256)
        fallidos = [i for i in asignados if i not in confirmados]
        if fallidos:
            self._evento("subida_parcial", f"Subida de '{nombre}' confirmada solo en {confirmados} "
                                           f"(falló en {fallidos})", logging.WARNING)
        else:
            self._evento("archivo_subido", f"Subida de '{nombre}' confirmada en {confirmados} "
                                           f"({request.tamano_bytes} bytes)")
        return pb.RespuestaRegistro(
            ok=True, mensaje=f"'{nombre}' registrado en {', '.join(confirmados)}")

    def EliminarArchivo(self, request, context):
        nombre = os.path.basename(request.nombre)
        with self.candado:
            replicas = self.indice.replicas(nombre)
            if replicas is None:
                context.abort(grpc.StatusCode.NOT_FOUND,
                              f"El coordinador no conoce el archivo '{nombre}'")
            destinos = [(i, self.nodos[i]["direccion"]) for i in replicas if i in self.nodos]

        # Se pide a cada nodo que borre su copia (fuera del candado: son llamadas de red).
        eliminados, fallidos = [], []
        for nodo_id, direccion in destinos:
            try:
                with grpc.insecure_channel(direccion) as canal:
                    pb_grpc.NodoAlmacenamientoStub(canal).EliminarArchivo(
                        pb.SolicitudArchivo(nombre=nombre), timeout=config.PLAZO)
                eliminados.append(nodo_id)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.NOT_FOUND:
                    eliminados.append(nodo_id)   # el nodo ya no lo tenía
                else:
                    fallidos.append(nodo_id)

        with self.candado:
            self.indice.eliminar_archivo(nombre)
        if fallidos:
            self._evento("copia_huerfana", f"Archivo '{nombre}' eliminado de {eliminados}; quedó una "
                                           f"copia huérfana en {fallidos} (nodo no disponible)",
                         logging.WARNING)
        else:
            self._evento("archivo_eliminado", f"Archivo '{nombre}' eliminado de {eliminados}")
        return pb.ResultadoEliminacion(nombre=nombre, nodos_eliminados=eliminados,
                                       nodos_fallidos=fallidos)

    def ListarArchivos(self, request, context):
        entradas = [pb.EntradaArchivo(nombre=n, nodos=r) for n, r in self.indice.archivos().items()]
        return pb.ListaArchivos(archivos=entradas)

    def EstadoNodos(self, request, context):
        carga = self.indice.carga()
        with self.candado:
            nodos = [pb.EstadoNodo(nodo_id=i, direccion=n["direccion"], vivo=n["vivo"],
                                   archivos=carga.get(i, 0))
                     for i, n in sorted(self.nodos.items())]
        return pb.ListaNodos(nodos=nodos)

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
                            pb.SolicitudPing(origen="coordinador"), timeout=PLAZO_PING)
                    vivo = True
                except grpc.RpcError:
                    vivo = False
                cambio = False
                with self.candado:
                    visto = nodo_id in self.vistos
                    if vivo:
                        self.vistos.add(nodo_id)
                    if nodo_id in self.nodos and self.nodos[nodo_id]["vivo"] != vivo:
                        self.nodos[nodo_id]["vivo"] = vivo
                        cambio = True
                if cambio:
                    if vivo and not visto:
                        self._evento("nodo_conectado", f"Nodo {nodo_id} conectado")
                    elif vivo:
                        self._evento("nodo_volvio", f"Nodo {nodo_id} VOLVIÓ a estar disponible",
                                     logging.WARNING)
                    else:
                        self._evento("nodo_caido", f"Nodo {nodo_id} NO RESPONDE (caído)",
                                     logging.WARNING)
            time.sleep(INTERVALO_SALUD)


def main():
    parser = argparse.ArgumentParser(description="Coordinador gRPC")
    parser.add_argument("--puerto", type=int, default=config.PUERTO_COORDINADOR)
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