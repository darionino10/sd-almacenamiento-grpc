"""Gateway web del sistema de almacenamiento distribuido.

El navegador habla HTTP con este gateway, y el gateway habla gRPC con el
coordinador y los nodos. Es un cliente más del sistema: no reemplaza a gRPC.

Uso:
    uvicorn gateway:app --reload --port 8000
    Abrir http://localhost:8000  (documentación de la API en /docs)
"""
import asyncio
import hashlib
import json
import os

import grpc
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import (FileResponse, JSONResponse, RedirectResponse, Response,
                               StreamingResponse)
from pydantic import BaseModel, Field

import almacenamiento_pb2 as pb
import almacenamiento_pb2_grpc as pb_grpc
import config
import cuentas
from config import PLAZO, PLAZO_ARCHIVO, TAMANO_FRAGMENTO

# El gateway corre en el mismo equipo que el coordinador
COORDINADOR = f"localhost:{config.PUERTO_COORDINADOR}"

# Traducción de códigos gRPC a códigos HTTP
CODIGOS_HTTP = {
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.INVALID_ARGUMENT: 400,
    grpc.StatusCode.UNAVAILABLE: 503,
    grpc.StatusCode.DEADLINE_EXCEEDED: 504,
}

app = FastAPI(
    title="Almacenamiento distribuido con gRPC",
    description=("Gateway HTTP del sistema. Cada petición se traduce a llamadas gRPC al "
                 "coordinador y a los nodos de almacenamiento. Los códigos de error de gRPC "
                 "se convierten a HTTP: NOT_FOUND → 404, UNAVAILABLE → 503, "
                 "DEADLINE_EXCEEDED → 504."),
    version="1.0.0",
    openapi_tags=[
        {"name": "Nodos", "description": "Estado de los nodos de almacenamiento"},
        {"name": "Archivos", "description": "Subir, consultar, descargar y eliminar archivos"},
        {"name": "Cuentas", "description": "Crear cuenta, iniciar y cerrar sesión"},
    ],
)

cuentas.iniciar_bd()
COOKIE = "sesion"
CARPETA_STATIC = os.path.join(os.path.dirname(__file__), "static")
# Rutas de la API que se pueden usar sin haber iniciado sesión
RUTAS_PUBLICAS = {"/api/cuentas", "/api/sesion"}


@app.middleware("http")
async def exigir_sesion(request: Request, siguiente):
    """Toda la API, salvo crear cuenta e iniciar sesión, exige una sesión válida."""
    ruta = request.url.path
    if ruta.startswith("/api/") and ruta not in RUTAS_PUBLICAS:
        usuario = cuentas.usuario_de_sesion(request.cookies.get(COOKIE))
        if not usuario:
            return JSONResponse(status_code=401, content={"detail": "Inicia sesión para continuar"})
        request.state.usuario = usuario["usuario"]   # para enviarlo al coordinador
    return await siguiente(request)


def metadatos(request: Request, accion: str = ""):
    """Metadatos gRPC con el usuario de la sesión (y la acción, si aplica)."""
    datos = [("usuario", request.state.usuario)]
    if accion:
        datos.append(("accion", accion))
    return datos


# ---------------- modelos de respuesta (documentación de /docs) ----------------
class Error(BaseModel):
    detail: str = Field(examples=["[NOT_FOUND] El coordinador no conoce el archivo 'x.pdf'"])


class Nodo(BaseModel):
    id: str = Field(examples=["nodo1"])
    direccion: str = Field(examples=["100.123.23.123:50051"])
    vivo: bool
    archivos: int = Field(description="archivos asignados al nodo")


class Archivo(BaseModel):
    nombre: str
    replicas: list[str] = Field(examples=[["nodo1", "nodo2"]])


class ResultadoNodo(BaseModel):
    nodo: str
    ok: bool
    bytes: int | None = None
    fragmentos: int | None = None
    hash_ok: bool | None = None
    error: str | None = Field(None, examples=["UNAVAILABLE"])


class ResultadoSubida(BaseModel):
    nombre: str
    sha256: str
    resultados: list[ResultadoNodo]
    confirmados: list[str] = Field(description="nodos donde el coordinador registró la réplica")


class Replica(BaseModel):
    nodo: str
    bytes: int | None = None
    sha256: str | None = None
    fecha: str | None = None
    error: str | None = None


class ResultadoEliminacion(BaseModel):
    nombre: str
    eliminados: list[str]
    fallidos: list[str] = Field(description="nodos caídos donde quedó una copia sin borrar")


class Usuario(BaseModel):
    usuario: str = Field(examples=["senendarionino10"])
    rol: str = Field(examples=["administrador"])


class DatosCuenta(BaseModel):
    usuario: str = Field(examples=["senendarionino10"],
                         description="Nombres y apellidos todo pegado, solo letras, y opcionalmente "
                                     "un número del 1 al 100 al final")
    contrasena: str = Field(examples=["NubeAzul.Tunja7"],
                            description="Mínimo 10 caracteres, con una mayúscula, una minúscula y un "
                                        "número; no puede contener el usuario ni ser una contraseña común")

class DatosSesion(BaseModel):
    usuario: str
    contrasena: str


# Respuestas de error comunes, para que /docs las muestre en cada endpoint
NO_ENCONTRADO = {404: {"model": Error, "description": "El archivo no existe (gRPC NOT_FOUND)"}}
NO_DISPONIBLE = {401: {"model": Error, "description": "No hay una sesión iniciada"},
                 503: {"model": Error, "description": "Coordinador o nodos no disponibles "
                                                      "(gRPC UNAVAILABLE)"},
                 504: {"model": Error, "description": "Se agotó el plazo (gRPC DEADLINE_EXCEEDED)"}}


def error_http(e: grpc.RpcError):
    """Convierte un error gRPC en una respuesta HTTP con su código y mensaje."""
    return HTTPException(status_code=CODIGOS_HTTP.get(e.code(), 500),
                         detail=f"[{e.code().name}] {e.details()}")


def coordinador():
    return pb_grpc.CoordinadorStub(grpc.insecure_channel(COORDINADOR))


# ---------------- página ----------------
@app.get("/", include_in_schema=False)
def inicio(request: Request):
    if not cuentas.usuario_de_sesion(request.cookies.get(COOKIE)):
        return RedirectResponse("/entrar")
    return FileResponse(os.path.join(CARPETA_STATIC, "index.html"))


@app.get("/entrar", include_in_schema=False)
def pagina_entrar(request: Request):
    if cuentas.usuario_de_sesion(request.cookies.get(COOKIE)):
        return RedirectResponse("/")
    return FileResponse(os.path.join(CARPETA_STATIC, "entrar.html"))


@app.get("/favicon.ico", include_in_schema=False)
@app.get("/apple-touch-icon.png", include_in_schema=False)
@app.get("/apple-touch-icon-precomposed.png", include_in_schema=False)
def sin_icono():
    return Response(status_code=204)


# ---------------- cuentas y sesión ----------------
def _poner_cookie(respuesta: Response, token: str):
    respuesta.set_cookie(COOKIE, token, httponly=True, samesite="lax",
                         max_age=config.DURACION_SESION_HORAS * 3600)


@app.post("/api/cuentas", tags=["Cuentas"], response_model=Usuario, status_code=201,
          responses={400: {"model": Error, "description": "Datos no válidos o usuario repetido"}},
          summary="Crear una cuenta")
def crear_cuenta(datos: DatosCuenta):
    """Crea la cuenta sin iniciar sesión. La primera cuenta del sistema queda como administrador."""
    try:
        return cuentas.crear_cuenta(datos.usuario, datos.contrasena)
    except cuentas.ErrorCuenta as e:
        raise HTTPException(400, str(e))

@app.post("/api/sesion", tags=["Cuentas"], response_model=Usuario,
          responses={401: {"model": Error, "description": "Usuario o contraseña incorrectos"}},
          summary="Iniciar sesión")
def iniciar_sesion(datos: DatosSesion, respuesta: Response):
    try:
        token, usuario = cuentas.iniciar_sesion(datos.usuario, datos.contrasena)
    except cuentas.ErrorCuenta as e:
        raise HTTPException(401, str(e))
    _poner_cookie(respuesta, token)
    return usuario


@app.get("/api/sesion", tags=["Cuentas"], response_model=Usuario,
         responses={401: {"model": Error, "description": "No hay una sesión iniciada"}},
         summary="Usuario de la sesión actual")
def sesion_actual(request: Request):
    usuario = cuentas.usuario_de_sesion(request.cookies.get(COOKIE))
    if not usuario:
        raise HTTPException(401, "Inicia sesión para continuar")
    return usuario


@app.delete("/api/sesion", tags=["Cuentas"], status_code=204, summary="Cerrar sesión")
def cerrar_sesion(request: Request):
    cuentas.cerrar_sesion(request.cookies.get(COOKIE))
    respuesta = Response(status_code=204)
    respuesta.delete_cookie(COOKIE)
    return respuesta


# ---------------- API ----------------
@app.get("/api/nodos", tags=["Nodos"], response_model=list[Nodo], responses=NO_DISPONIBLE,
         summary="Estado de los nodos")
def estado_nodos():
    """Estado de cada nodo según el coordinador."""
    try:
        r = coordinador().EstadoNodos(pb.Vacio(), timeout=PLAZO)
    except grpc.RpcError as e:
        raise error_http(e)
    return [{"id": n.nodo_id, "direccion": n.direccion, "vivo": n.vivo,
             "archivos": n.archivos} for n in r.nodos]


@app.get("/api/archivos", tags=["Archivos"], response_model=list[Archivo],
         responses=NO_DISPONIBLE, summary="Listar archivos y réplicas")
def listar_archivos():
    """Archivos registrados en el coordinador y sus réplicas."""
    try:
        r = coordinador().ListarArchivos(pb.Vacio(), timeout=PLAZO)
    except grpc.RpcError as e:
        raise error_http(e)
    return [{"nombre": a.nombre, "replicas": list(a.nodos)} for a in r.archivos]


@app.delete("/api/archivos/{nombre}", tags=["Archivos"], response_model=ResultadoEliminacion,
            responses={**NO_ENCONTRADO, **NO_DISPONIBLE}, summary="Eliminar un archivo")
def eliminar(nombre: str, request: Request):

    """Elimina el archivo de todas sus réplicas y del índice del coordinador."""
    try:
        r = coordinador().EliminarArchivo(pb.SolicitudArchivo(nombre=nombre), timeout=PLAZO * 3,
                                          metadata=metadatos(request))
    except grpc.RpcError as e:
        raise error_http(e)
    return {"nombre": r.nombre, "eliminados": list(r.nodos_eliminados),
            "fallidos": list(r.nodos_fallidos)}


@app.post("/api/subir", tags=["Archivos"], response_model=ResultadoSubida,
          responses={400: {"model": Error, "description": "El archivo no tiene nombre"},
                     **NO_DISPONIBLE},
          summary="Subir un archivo a dos réplicas")
def subir(request: Request, archivo: UploadFile = File(...)):
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
            timeout=PLAZO, metadata=metadatos(request))
    except grpc.RpcError as e:
        raise error_http(e)
    return {"nombre": nombre, "sha256": esperado, "resultados": resultados,
            "confirmados": nodos_ok}


@app.get("/api/info/{nombre}", tags=["Archivos"], response_model=list[Replica],
         responses={**NO_ENCONTRADO, **NO_DISPONIBLE}, summary="Metadatos en cada réplica")
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


@app.get("/api/descargar/{nombre}", tags=["Archivos"],
         responses={200: {"content": {"application/octet-stream": {}},
                          "description": "Contenido del archivo. La cabecera X-Nodo-Origen "
                                         "indica la réplica usada y X-Nodos-Fallidos las que "
                                         "no respondieron."},
                    **NO_ENCONTRADO, **NO_DISPONIBLE},
         summary="Descargar desde la primera réplica disponible")
def descargar(nombre: str, request: Request):
    """Descarga desde la primera réplica disponible; si una falla, prueba la siguiente."""
    try:
        ubicacion = coordinador().UbicarArchivo(
            pb.SolicitudUbicacion(nombre=nombre, operacion=pb.DESCARGAR), timeout=PLAZO,
            metadata=metadatos(request, "descarga"))
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

    
@app.get("/api/eventos", tags=["Nodos"], responses={401: {"model": Error}},
         summary="Actividad del sistema en tiempo real (Server-Sent Events)")
async def eventos(request: Request):
    """Reenvía al navegador la suscripción gRPC SuscribirEventos del coordinador.
    Primero llegan los últimos 50 eventos y luego cada evento nuevo al instante."""
    canal = grpc.insecure_channel(COORDINADOR)
    flujo = pb_grpc.CoordinadorStub(canal).SuscribirEventos(pb.SolicitudEventos(ultimos=50))

    async def emitir():
        try:
            while not await request.is_disconnected():
                try:
                    evento = await asyncio.to_thread(next, flujo, None)
                except grpc.RpcError:
                    yield "event: caida\ndata: {}\n\n"   # el coordinador no está disponible
                    break
                if evento is None:
                    break
                if evento.tipo == "latido":
                    yield ": latido\n\n"                  # comentario SSE: mantiene la conexión
                    continue
                datos = {"id": evento.id, "fecha": evento.fecha, "tipo": evento.tipo,
                         "detalle": evento.detalle, "usuario": evento.usuario}
                yield f"id: {evento.id}\ndata: {json.dumps(datos, ensure_ascii=False)}\n\n"
        finally:
            flujo.cancel()
            canal.close()

    return StreamingResponse(emitir(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})