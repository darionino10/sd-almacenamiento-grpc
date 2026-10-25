"""Cuentas de usuario y sesiones del panel web.

Guarda los usuarios y las sesiones en una base SQLite propia del gateway.
Las contraseñas nunca se guardan: se guarda su hash con scrypt y una sal
aleatoria por usuario. Los tokens de sesión tampoco se guardan tal cual,
solo su hash SHA-256.
"""
import hashlib
import hmac
import os
import re
import secrets
import sqlite3
import threading
from datetime import datetime, timedelta

import config

RUTA_BD = os.path.join(config.CARPETA_GATEWAY, "usuarios.db")
# Nombres y apellidos todo pegado (solo letras) y, opcionalmente, un número del 1 al 100 al final
PATRON_USUARIO = re.compile(r"^[a-z]{4,40}([1-9][0-9]?|100)?$")
_candado = threading.Lock()


class ErrorCuenta(Exception):
    """Error de validación al crear una cuenta o iniciar sesión."""


def _conexion():
    os.makedirs(config.CARPETA_GATEWAY, exist_ok=True)
    con = sqlite3.connect(RUTA_BD)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con


def iniciar_bd():
    """Crea las tablas si no existen."""
    with _candado, _conexion() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario     TEXT    NOT NULL UNIQUE,
                sal         BLOB    NOT NULL,
                hash        BLOB    NOT NULL,
                rol         TEXT    NOT NULL DEFAULT 'usuario',
                creado      TEXT    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sesiones (
                token_hash  TEXT    PRIMARY KEY,
                usuario_id  INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
                creada      TEXT    NOT NULL,
                expira      TEXT    NOT NULL
            );
        """)


# ---------------- contraseñas ----------------
def _hash_contrasena(contrasena, sal):
    return hashlib.scrypt(contrasena.encode("utf-8"), salt=sal, n=2**14, r=8, p=1, dklen=32)


def _hash_token(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# ---------------- cuentas ----------------
def validar_contrasena(contrasena):
    """Mínimo 8 caracteres, con al menos una mayúscula, una minúscula y un número."""
    faltan = []
    if len(contrasena) < 8:
        faltan.append("8 caracteres")
    if not re.search(r"[A-Z]", contrasena):
        faltan.append("una mayúscula")
    if not re.search(r"[a-z]", contrasena):
        faltan.append("una minúscula")
    if not re.search(r"[0-9]", contrasena):
        faltan.append("un número")
    if faltan:
        raise ErrorCuenta("La contraseña necesita al menos " + ", ".join(faltan))


def crear_cuenta(usuario, contrasena):
    """Crea una cuenta. La primera cuenta del sistema queda como administrador."""
    usuario = usuario.strip().lower()
    if not PATRON_USUARIO.match(usuario):
        raise ErrorCuenta("El usuario son tus nombres y apellidos todo pegado, solo letras "
                          "(sin tildes, espacios ni signos), y si quieres un número del 1 al 100 "
                          "al final. Ejemplo: senendarionino10")
    validar_contrasena(contrasena)

    sal = secrets.token_bytes(16)
    clave = _hash_contrasena(contrasena, sal)
    with _candado, _conexion() as con:
        primera = con.execute("SELECT COUNT(*) FROM usuarios").fetchone()[0] == 0
        rol = "administrador" if primera else "usuario"
        try:
            con.execute(
                "INSERT INTO usuarios (usuario, sal, hash, rol, creado) VALUES (?, ?, ?, ?, ?)",
                (usuario, sal, clave, rol, datetime.now().isoformat(timespec="seconds")))
        except sqlite3.IntegrityError:
            raise ErrorCuenta("Ese usuario ya existe; elige otro") from None
    return {"usuario": usuario, "rol": rol}


# ---------------- sesiones ----------------
def iniciar_sesion(usuario, contrasena):
    """Comprueba la contraseña y devuelve (token, datos del usuario)."""
    usuario = usuario.strip().lower()
    with _candado, _conexion() as con:
        fila = con.execute("SELECT * FROM usuarios WHERE usuario = ?", (usuario,)).fetchone()
        # Se calcula el hash aunque el usuario no exista, para no revelarlo por el tiempo de respuesta
        sal = fila["sal"] if fila else secrets.token_bytes(16)
        calculado = _hash_contrasena(contrasena, sal)
        if not fila or not hmac.compare_digest(calculado, fila["hash"]):
            raise ErrorCuenta("Usuario o contraseña incorrectos")

        token = secrets.token_urlsafe(32)
        ahora = datetime.now()
        con.execute("DELETE FROM sesiones WHERE expira < ?", (ahora.isoformat(),))
        con.execute(
            "INSERT INTO sesiones (token_hash, usuario_id, creada, expira) VALUES (?, ?, ?, ?)",
            (_hash_token(token), fila["id"], ahora.isoformat(timespec="seconds"),
             (ahora + timedelta(hours=config.DURACION_SESION_HORAS)).isoformat(timespec="seconds")))
    return token, {"usuario": fila["usuario"], "rol": fila["rol"]}


def usuario_de_sesion(token):
    """Devuelve el usuario dueño de un token vigente, o None."""
    if not token:
        return None
    with _candado, _conexion() as con:
        fila = con.execute(
            "SELECT u.usuario, u.rol, s.expira FROM sesiones s "
            "JOIN usuarios u ON u.id = s.usuario_id WHERE s.token_hash = ?",
            (_hash_token(token),)).fetchone()
    if not fila or fila["expira"] < datetime.now().isoformat():
        return None
    return {"usuario": fila["usuario"], "rol": fila["rol"]}


def cerrar_sesion(token):
    if token:
        with _candado, _conexion() as con:
            con.execute("DELETE FROM sesiones WHERE token_hash = ?", (_hash_token(token),))