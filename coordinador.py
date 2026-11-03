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
import queue
import threading
import time
from concurrent import futures

import grpc

import almacenamiento_pb2 as pb
import almacenamiento_pb2_grpc as pb_grpc
import config
from config import ESPERA_RE_REPLICACION, INTERVALO_SALUD, PLAZO_PING, REPLICAS
from indice import Indice

CARPETA = "datos_coordinador"
RUTA_BD = os.path.join(CARPETA, "coordinador.db")
RUTA_JSON_ANTERIOR = os.path.join(CARPETA, "indice.json")
LATIDO = 10   # segundos sin eventos tras los que se envía un "latido" para mantener viva la conexión


def lista(nodos):
    """['nodo3', 'nodo1'] -> 'NODO3 y NODO1'"""
    nombres = [n.upper() for n in nodos]
    return " y ".join([", ".join(nombres[:-1]), nombres[-1]]) if len(nombres) > 1 else "".join(nombres)


def usuario_de(context):
    """Usuario que hizo la llamada, enviado por el gateway o el cliente en los metadatos gRPC."""
    return dict(context.invocation_metadata()).get("usuario", "anónimo")


class CoordinadorServicer(pb_grpc.CoordinadorServicer):

    def __init__(self):
        self.candado = threading.Lock()
        self.indice = Indice(RUTA_BD)
        migrados = self.indice.migrar_json(RUTA_JSON_ANTERIOR)
        self.suscriptores = set()            # colas de los paneles suscritos a eventos
        self.candado_suscriptores = threading.Lock()
        if migrados:
            self._evento("migracion", f"Se importaron {migrados} archivo(s) desde indice.json")
        # El estado vivo/caído solo vive en memoria: al arrancar, todos empiezan como caídos
        # hasta que se registran o responden al ping.
        self.nodos = {i: {"direccion": d, "vivo": False} for i, d in self.indice.nodos().items()}
        self.pendientes = {}   # nombre -> nodos asignados que aún no confirman la subida
        self.vistos = set()    # nodos que ya respondieron en esta sesión del coordinador

        # Desde cuándo está caído cada nodo (para el periodo de gracia de la re-replicación).
        # Se usa time.monotonic(): un reloj que solo avanza y no cambia si ajustan la hora del PC.
        # Al arrancar, los nodos conocidos cuentan como caídos hasta que se conecten.
        ahora = time.monotonic()
        self.caido_desde = {i: ahora for i in self.nodos}
        # Archivos que ya se avisaron como "en riesgo", para no repetir el aviso cada 5 segundos
        self.sin_destino = set()
                # Nodos que volvieron y se están revisando: aún no se confía en sus copias
        self.resincronizando = set()

        self._evento("coordinador_iniciado",
                     f"Coordinador iniciado con {len(self.nodos)} nodo(s) conocido(s)")

    def _evento(self, tipo, detalle, nivel=logging.INFO, usuario=""):
        """Guarda el evento en la bitácora (SQLite), lo muestra en la terminal y lo envía
        a todos los suscriptores (paneles abiertos)."""
        evento = self.indice.evento(tipo, detalle, usuario)
        logging.log(nivel, f"{usuario} {detalle}" if usuario else detalle)
        with self.candado_suscriptores:
            for cola in self.suscriptores:
                cola.put(evento)

    def _info(self, nodo_id):
        return pb.InfoNodo(nodo_id=nodo_id, direccion=self.nodos[nodo_id]["direccion"])

    # ---------- métodos remotos ----------
    def RegistrarNodo(self, request, context):
        with self.candado:
            anterior = self.nodos.get(request.nodo_id)
            visto = request.nodo_id in self.vistos
            self.nodos[request.nodo_id] = {"direccion": request.direccion, "vivo": True}
            self.vistos.add(request.nodo_id)
            self.caido_desde.pop(request.nodo_id, None)   # si estaba caído, ya no lo está
            self.indice.guardar_nodo(request.nodo_id, request.direccion)

        if anterior is None:
            self._evento("nodo_registrado",
                         f"{request.nodo_id.upper()} se registró en {request.direccion}")
        elif not visto:
            # Ya era conocido, pero es su primera conexión desde que arrancó el coordinador
            self._evento("nodo_conectado", f"{request.nodo_id.upper()} conectado")
            self._iniciar_resincronizacion(request.nodo_id)
        elif not anterior["vivo"]:
            self._evento("nodo_volvio", f"{request.nodo_id.upper()} volvió a estar disponible",
                         logging.WARNING)
            self._iniciar_resincronizacion(request.nodo_id)

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

            # DESCARGAR (también se usa para consultar la información de un archivo)
            replicas = self.indice.replicas(nombre)
            if replicas is None:
                context.abort(grpc.StatusCode.NOT_FOUND,
                              f"El coordinador no conoce el archivo '{nombre}'")
            # Primero los nodos vivos; los caídos quedan al final como último intento.
            ordenados = sorted(replicas, key=lambda i: not self.nodos.get(i, {}).get("vivo"))
            ubicacion = pb.Ubicacion(nombre=nombre,
                                     nodos=[self._info(i) for i in ordenados if i in self.nodos])
        if dict(context.invocation_metadata()).get("accion") == "descarga":
            self._evento("archivo_descargado", f"descargó '{nombre}'", usuario=usuario_de(context))
        return ubicacion

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
        usuario = usuario_de(context)
        if fallidos:
            self._evento("subida_parcial", f"subió '{nombre}' solo a {lista(confirmados)}; "
                                           f"falló en {lista(fallidos)}", logging.WARNING, usuario)
        else:
            self._evento("archivo_subido", f"subió '{nombre}' a {lista(confirmados)} "
                                           f"({request.tamano_bytes} bytes)", usuario=usuario)
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
        usuario = usuario_de(context)
        if fallidos:
            self._evento("copia_huerfana", f"eliminó '{nombre}' de {lista(eliminados) or 'ningún nodo'}; "
                                           f"quedó una copia en {lista(fallidos)} (nodo caído)",
                         logging.WARNING, usuario)
        else:
            self._evento("archivo_eliminado", f"eliminó '{nombre}' de {lista(eliminados)}",
                         usuario=usuario)
        return pb.ResultadoEliminacion(nombre=nombre, nodos_eliminados=eliminados,
                                       nodos_fallidos=fallidos)

    def SuscribirEventos(self, request, context):
        """Streaming del servidor: historial reciente y luego cada evento nuevo."""
        cola = queue.Queue()
        with self.candado_suscriptores:
            self.suscriptores.add(cola)
        logging.info("Nuevo suscriptor de eventos (%d en total)", len(self.suscriptores))
        try:
            for evento in reversed(self.indice.eventos(request.ultimos or 50)):
                yield pb.Evento(**evento)
            while context.is_active():
                try:
                    evento = cola.get(timeout=LATIDO)
                except queue.Empty:
                    yield pb.Evento(tipo="latido")   # mantiene la conexión abierta
                    continue
                yield pb.Evento(**evento)
        finally:
            with self.candado_suscriptores:
                self.suscriptores.discard(cola)

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
                nodos_actuales = list(self.nodos.items())
            for nodo_id, nodo in nodos_actuales:
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
                    # Solo hay cambio si el estado nuevo es distinto al que tenía
                    if nodo_id in self.nodos and self.nodos[nodo_id]["vivo"] != vivo:
                        self.nodos[nodo_id]["vivo"] = vivo
                        cambio = True
                        if vivo:
                            # Volvió: se borra su hora de caída (cancela la re-replicación pendiente)
                            self.caido_desde.pop(nodo_id, None)
                        else:
                            # Se cayó: se anota la hora; el periodo de gracia empieza a contar aquí
                            self.caido_desde[nodo_id] = time.monotonic()
                if cambio:
                    if vivo and not visto:
                        self._evento("nodo_conectado", f"{nodo_id.upper()} conectado")
                    elif vivo:
                        self._evento("nodo_volvio", f"{nodo_id.upper()} volvió a estar disponible",
                                     logging.WARNING)
                    else:
                        self._evento("nodo_caido", f"{nodo_id.upper()} dejó de responder",
                                     logging.WARNING)
                    if vivo:
                        # Volvió (o se conectó): revisar sus copias antes de confiar en ellas
                        self._iniciar_resincronizacion(nodo_id)
            time.sleep(INTERVALO_SALUD)

    # ---------- re-replicación automática ----------
    def reparar(self):
        """Busca archivos con menos copias vivas de las debidas y ordena copiarlos.
        Se ejecuta en su propio hilo, cada INTERVALO_SALUD segundos."""
        while True:
            time.sleep(INTERVALO_SALUD)
            try:
                self._revisar_replicas()
            except Exception:
                # Un error inesperado no debe matar el hilo: se registra y se reintenta luego
                logging.exception("Error en la re-replicación; se reintentará")

    def _revisar_replicas(self):
        ahora = time.monotonic()
        # Se toma una "foto" del estado con el candado, y se suelta antes de hacer llamadas de red
        with self.candado:
            vivos = {i for i, n in self.nodos.items() if n["vivo"]}
            # Caídos hace menos de 30 s: aún dentro del periodo de gracia, puede que vuelvan
            en_gracia = {i for i, t in self.caido_desde.items()
                         if ahora - t < ESPERA_RE_REPLICACION}
            direcciones = {i: n["direccion"] for i, n in self.nodos.items()}
            ocupados = set(self.resincronizando)   # nodos que volvieron y aún no se revisan
        carga = self.indice.carga()   # cuántos archivos tiene cada nodo (para repartir)
        for nombre, replicas in self.indice.archivos().items():
            vivas = [r for r in replicas if r in vivos]           # copias que responden
            esperando = [r for r in replicas if r in en_gracia]   # copias que quizá vuelvan
            # Sobre-replicación: más copias vivas de las debidas (pasa cuando vuelve un nodo
            # cuyo archivo ya se había re-replicado). Solo se actúa si todas ya fueron revisadas.
            if len(vivas) > REPLICAS and not ocupados.intersection(vivas):
                self._quitar_sobrante(nombre, vivas, carga, direcciones)
                continue
            # Bien replicado, o con copias que aún pueden volver: no se hace nada todavía
            if len(vivas) >= REPLICAS or len(vivas) + len(esperando) >= REPLICAS:
                self.sin_destino.discard(nombre)
                continue
            if not vivas:
                continue   # no queda ninguna copia viva desde la cual copiar

            #(se excluyen los nodos en revisión: todavía no se sabe qué tienen)
            candidatos = sorted((n for n in vivos if n not in replicas and n not in ocupados),
                                key=lambda n: carga.get(n, 0))
            if not candidatos:
                # No hay a dónde copiar: se avisa una sola vez y se reintenta en cada ciclo
                if nombre not in self.sin_destino:
                    self.sin_destino.add(nombre)
                    self._evento("replica_en_riesgo", f"'{nombre}' tiene solo {len(vivas)} copia(s) "
                                 "viva(s) y no hay otro nodo disponible para copiarlo",
                                 logging.WARNING)
                continue

            origen, destino = vivas[0], candidatos[0]
            self._copiar(nombre, origen, destino, direcciones)
            # Se suma la copia nueva a la carga, para que el siguiente archivo vaya a otro nodo
            carga[destino] = carga.get(destino, 0) + 1

    def _copiar(self, nombre, origen, destino, direcciones):
        """Ordena al nodo origen enviar el archivo al destino y verifica la copia."""
        logging.info("Re-replicando '%s' de %s a %s", nombre, origen, destino)
        try:
            # Plano de control: el coordinador solo envía la orden (nombre + dirección destino);
            # los bytes viajan directo de nodo a nodo (ReplicarHacia en nodo.py)
            with grpc.insecure_channel(direcciones[origen]) as canal:
                r = pb_grpc.NodoAlmacenamientoStub(canal).ReplicarHacia(
                    pb.SolicitudReplicacion(nombre=nombre, destino=direcciones[destino]),
                    timeout=config.PLAZO_ARCHIVO * 2)
        except grpc.RpcError as e:
            # Falló el origen o el destino: no se registra nada y se reintenta en el próximo ciclo
            logging.warning("No se pudo re-replicar '%s' (%s); se reintentará", nombre, e.details())
            return
        # Verificación de integridad: el hash de la copia debe ser igual al de la subida original
        esperado = self.indice.hash_de(nombre)
        if esperado and r.hash_sha256 != esperado:
            logging.warning("La copia de '%s' en %s no coincide con el hash original; "
                            "no se registra", nombre, destino)
            return
        # Solo ahora, con la copia verificada, se registra la réplica nueva en SQLite
        self.indice.agregar_replica(nombre, destino, r.hash_sha256)
        self.sin_destino.discard(nombre)
        self._evento("re_replicado", f"'{nombre}' se re-replicó de {origen.upper()} a "
                                     f"{destino.upper()} ({r.tamano_bytes} bytes, hash verificado)")

            # ---------- resincronización al volver un nodo ----------
    def _iniciar_resincronizacion(self, nodo_id):
        """Lanza la revisión del nodo en un hilo aparte, para no frenar el ping ni el registro."""
        with self.candado:
            if nodo_id in self.resincronizando:
                return                          # ya se está revisando: no se repite
            self.resincronizando.add(nodo_id)
        threading.Thread(target=self._resincronizar, args=(nodo_id,), daemon=True).start()

    def _resincronizar(self, nodo_id):
        """Compara lo que el nodo tiene en disco con lo que dice el índice y lo corrige."""
        try:
            with self.candado:
                direccion = self.nodos[nodo_id]["direccion"]
                pendientes = set(self.pendientes)   # subidas en curso: no se tocan
            # Paso 1: pedir el inventario real del nodo (archivos en disco + hash recalculado)
            try:
                with grpc.insecure_channel(direccion) as canal:
                    inventario = pb_grpc.NodoAlmacenamientoStub(canal).Inventario(
                        pb.Vacio(), timeout=config.PLAZO_ARCHIVO)
            except grpc.RpcError as e:
                logging.warning("No se pudo resincronizar %s (%s)", nodo_id, e.code().name)
                return

            en_disco = {a.nombre: a.hash_sha256 for a in inventario.archivos}   # nombre -> hash
            segun_indice = self.indice.archivos()                               # nombre -> réplicas
            huerfanas, danadas, perdidas = [], [], []

            # Paso 2: revisar lo que el nodo SÍ tiene
            for nombre, sha in en_disco.items():
                if nombre in pendientes:
                    continue
                if nodo_id not in segun_indice.get(nombre, []):
                    # El índice no cuenta esta copia: se eliminó o no se confirmó mientras estaba caído
                    huerfanas.append(nombre)
                elif self.indice.hash_de(nombre) and sha != self.indice.hash_de(nombre):
                    # El contenido ya no es el original: copia corrupta
                    danadas.append(nombre)

            # Paso 3: revisar lo que el índice dice que tiene pero ya NO está en su disco
            for nombre, replicas in segun_indice.items():
                if nodo_id in replicas and nombre not in en_disco:
                    perdidas.append(nombre)

            # Paso 4: corregir
            for nombre in huerfanas + danadas:
                self._borrar_en_nodo(direccion, nombre)       # se borra del disco del nodo
            for nombre in danadas + perdidas:
                self.indice.quitar_replica(nombre, nodo_id)   # la re-replicación repondrá la copia

            # Paso 5: dejar constancia en la bitácora (solo si hubo algo que corregir)
            if huerfanas or danadas or perdidas:
                partes = []
                if huerfanas:
                    partes.append(f"{len(huerfanas)} copia(s) huérfana(s) eliminada(s)")
                if danadas:
                    partes.append(f"{len(danadas)} copia(s) dañada(s) descartada(s)")
                if perdidas:
                    partes.append(f"{len(perdidas)} copia(s) perdida(s) quitada(s) del índice")
                self._evento("resincronizado", f"{nodo_id.upper()} resincronizado: " + ", ".join(partes),
                             logging.WARNING)
            else:
                logging.info("%s revisado: sus copias coinciden con el índice", nodo_id)
        finally:
            # Pase lo que pase, el nodo deja de estar "en revisión"
            with self.candado:
                self.resincronizando.discard(nodo_id)

    def _borrar_en_nodo(self, direccion, nombre):
        """Pide a un nodo borrar un archivo; si ya no lo tenía, no pasa nada."""
        try:
            with grpc.insecure_channel(direccion) as canal:
                pb_grpc.NodoAlmacenamientoStub(canal).EliminarArchivo(
                    pb.SolicitudArchivo(nombre=nombre), timeout=config.PLAZO)
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.NOT_FOUND:
                logging.warning("No se pudo borrar '%s' en %s (%s)", nombre, direccion, e.code().name)

    def _quitar_sobrante(self, nombre, vivas, carga, direcciones):
        """Deja el archivo con REPLICAS copias, quitando la del nodo más cargado."""
        sobrante = max(vivas, key=lambda n: carga.get(n, 0))
        self._borrar_en_nodo(direcciones[sobrante], nombre)
        self.indice.quitar_replica(nombre, sobrante)
        carga[sobrante] = carga.get(sobrante, 0) - 1   # actualizar la carga para el siguiente archivo
        self._evento("sobre_replicacion", f"'{nombre}' tenía {len(vivas)} copias vivas; se eliminó "
                                          f"la sobrante de {sobrante.upper()}")
def main():
    parser = argparse.ArgumentParser(description="Coordinador gRPC")
    parser.add_argument("--puerto", type=int, default=config.PUERTO_COORDINADOR)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [coordinador] %(message)s")

    servicer = CoordinadorServicer()
    # Cada panel abierto mantiene una suscripción de eventos ocupando un hilo
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    pb_grpc.add_CoordinadorServicer_to_server(servicer, servidor)
    servidor.add_insecure_port(f"[::]:{args.puerto}")
    servidor.start()
    logging.info("Coordinador escuchando en el puerto %d", args.puerto)

    threading.Thread(target=servicer.vigilar_nodos, daemon=True).start()
    threading.Thread(target=servicer.reparar, daemon=True).start()   # hilo de re-replicación
    servidor.wait_for_termination()

    


if __name__ == "__main__":
    main()