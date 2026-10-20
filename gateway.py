"""Gateway web del sistema de almacenamiento distribuido.

El navegador habla HTTP con este gateway, y el gateway habla gRPC con el
coordinador y los nodos. Es un cliente más del sistema: no reemplaza a gRPC.

Uso:
    uvicorn gateway:app --reload --port 8000
    Abrir http://localhost:8000  (documentación de la API en /docs)
"""
import hashlib
import os

import grpc
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse

import almacenamiento_pb2 as pb
import almacenamiento_pb2_grpc as pb_grpc

COORDINADOR = os.environ.get("COORDINADOR", "localhost:50050")
TAMANO_FRAGMENTO = 64 * 1024
PLAZO = 5
PLAZO_ARCHIVO = 30

# Traducción de códigos gRPC a códigos HTTP
CODIGOS_HTTP = {
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.INVALID_ARGUMENT: 400,
    grpc.StatusCode.UNAVAILABLE: 503,
    grpc.StatusCode.DEADLINE_EXCEEDED: 504,
}

app = FastAPI(title="Almacenamiento distribuido con gRPC",
              description="Gateway HTTP que traduce cada petición a llamadas gRPC")


def error_http(e: grpc.RpcError):
    """Convierte un error gRPC en una respuesta HTTP con su código y mensaje."""
    return HTTPException(status_code=CODIGOS_HTTP.get(e.code(), 500),
                         detail=f"[{e.code().name}] {e.details()}")


def coordinador():
    return pb_grpc.CoordinadorStub(grpc.insecure_channel(COORDINADOR))


# ---------------- página ----------------
@app.get("/", include_in_schema=False)
def inicio():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


# ---------------- API ----------------
@app.get("/api/nodos")
def estado_nodos():
    """Estado de cada nodo según el coordinador."""
    try:
        r = coordinador().EstadoNodos(pb.Vacio(), timeout=PLAZO)
    except grpc.RpcError as e:
        raise error_http(e)
    return [{"id": n.nodo_id, "direccion": n.direccion, "vivo": n.vivo,
             "archivos": n.archivos} for n in r.nodos]


@app.get("/api/archivos")
def listar_archivos():
    """Archivos registrados en el coordinador y sus réplicas."""
    try:
        r = coordinador().ListarArchivos(pb.Vacio(), timeout=PLAZO)
    except grpc.RpcError as e:
        raise error_http(e)
    return [{"nombre": a.nombre, "replicas": list(a.nodos)} for a in r.archivos]


@app.post("/api/subir")
def subir(archivo: UploadFile = File(...)):
    """Sube un archivo a los nodos que asigne el coordinador (streaming gRPC)."""
    nombre = os.path.basename(archivo.filename or "")
    if not nombre:
        raise HTTPException(400, "El archivo no tiene nombre")

    # Hash local para comprobar la integridad en cada nodo
    sha = hashlib.sha256()
    tamano = 0
    archivo.file.seek(0)
    while bloque := archivo.file.read(TAMANO_FRAGMENTO):
        sha.update(bloque)
        tamano += len(bloque)
    esperado = sha.hexdigest()

    try:
        ubicacion = coordinador().UbicarArchivo(
            pb.SolicitudUbicacion(nombre=nombre, operacion=pb.SUBIR), timeout=PLAZO)
    except grpc.RpcError as e:
        raise error_http(e)

    def fragmentos():
        archivo.file.seek(0)
        indice = 0
        while bloque := archivo.file.read(TAMANO_FRAGMENTO):
            yield pb.Fragmento(nombre=nombre, indice=indice, datos=bloque)
            indice += 1

    resultados = []
    for nodo in ubicacion.nodos:
        try:
            with grpc.insecure_channel(nodo.direccion) as canal:
                r = pb_grpc.NodoAlmacenamientoStub(canal).SubirArchivo(
                    fragmentos(), timeout=PLAZO_ARCHIVO)
            resultados.append({"nodo": r.nodo_id, "ok": True, "bytes": r.tamano_bytes,
                               "fragmentos": r.fragmentos,
                               "hash_ok": r.hash_sha256 == esperado})
        except grpc.RpcError as e:
            resultados.append({"nodo": nodo.nodo_id, "ok": False, "error": e.code().name})

    # Confirmar al coordinador solo los nodos que recibieron el archivo íntegro
    nodos_ok = [r["nodo"] for r in resultados if r["ok"] and r["hash_ok"]]
    if not nodos_ok:
        raise HTTPException(503, f"La subida de '{nombre}' falló en todos los nodos")
    try:
        coordinador().ConfirmarSubida(
            pb.ConfirmacionSubida(nombre=nombre, nodos_ok=nodos_ok,
                                  hash_sha256=esperado, tamano_bytes=tamano),
            timeout=PLAZO)
    except grpc.RpcError as e:
        raise error_http(e)
    return {"nombre": nombre, "sha256": esperado, "resultados": resultados,
            "confirmados": nodos_ok}

@app.get("/api/info/{nombre}")
def info(nombre: str):
    """Metadatos del archivo en cada una de sus réplicas."""
    try:
        ubicacion = coordinador().UbicarArchivo(
            pb.SolicitudUbicacion(nombre=nombre, operacion=pb.DESCARGAR), timeout=PLAZO)
    except grpc.RpcError as e:
        raise error_http(e)
    respuesta = []
    for nodo in ubicacion.nodos:
        try:
            with grpc.insecure_channel(nodo.direccion) as canal:
                m = pb_grpc.NodoAlmacenamientoStub(canal).ConsultarMetadatos(
                    pb.SolicitudArchivo(nombre=nombre), timeout=PLAZO)
            respuesta.append({"nodo": m.nodo_id, "bytes": m.tamano_bytes,
                              "sha256": m.hash_sha256, "fecha": m.fecha_subida})
        except grpc.RpcError as e:
            respuesta.append({"nodo": nodo.nodo_id, "error": e.code().name})
    return respuesta


@app.get("/api/descargar/{nombre}")
def descargar(nombre: str):
    """Descarga desde la primera réplica disponible; si una falla, prueba la siguiente."""
    try:
        ubicacion = coordinador().UbicarArchivo(
            pb.SolicitudUbicacion(nombre=nombre, operacion=pb.DESCARGAR), timeout=PLAZO)
    except grpc.RpcError as e:
        raise error_http(e)

    fallidos = []
    for nodo in ubicacion.nodos:
        canal = grpc.insecure_channel(nodo.direccion)
        flujo = pb_grpc.NodoAlmacenamientoStub(canal).DescargarArchivo(
            pb.SolicitudArchivo(nombre=nombre), timeout=PLAZO_ARCHIVO)
        try:
            primero = next(flujo)  # si el nodo está caído, falla aquí
        except StopIteration:
            primero = None
        except grpc.RpcError as e:
            fallidos.append(f"{nodo.nodo_id}:{e.code().name}")
            canal.close()
            continue

        def contenido(primero=primero, flujo=flujo, canal=canal):
            try:
                if primero is not None:
                    yield primero.datos
                for fragmento in flujo:
                    yield fragmento.datos
            finally:
                canal.close()

        cabeceras = {"Content-Disposition": f'attachment; filename="{nombre}"',
                     "X-Nodo-Origen": nodo.nodo_id,
                     "X-Nodos-Fallidos": ",".join(fallidos)}
        return StreamingResponse(contenido(), media_type="application/octet-stream",
                                 headers=cabeceras)

    raise HTTPException(503, f"Ninguna réplica respondió ({', '.join(fallidos)})")