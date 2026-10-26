"""Índice persistente del coordinador en SQLite.

Guarda los nodos conocidos, los archivos con sus réplicas y la bitácora de
eventos. Cada operación es una transacción: si el coordinador se cae a mitad
de una escritura, la base queda como estaba antes, nunca a medias.
"""
import json
import os
import sqlite3
from datetime import datetime


def _ahora():
    return datetime.now().isoformat(timespec="seconds")


class Indice:

    def __init__(self, ruta):
        self.ruta = ruta
        os.makedirs(os.path.dirname(ruta) or ".", exist_ok=True)
        with self._con() as con:
            con.execute("PRAGMA journal_mode = WAL")   # escrituras seguras ante cortes
            con.executescript("""
                CREATE TABLE IF NOT EXISTS nodos (
                    id          TEXT PRIMARY KEY,
                    direccion   TEXT NOT NULL,
                    registrado  TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS archivos (
                    nombre      TEXT PRIMARY KEY,
                    tamano      INTEGER,
                    sha256      TEXT,
                    subido      TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS replicas (
                    archivo     TEXT NOT NULL REFERENCES archivos(nombre) ON DELETE CASCADE,
                    nodo        TEXT NOT NULL,
                    PRIMARY KEY (archivo, nodo)
                );
                CREATE TABLE IF NOT EXISTS eventos (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    fecha       TEXT NOT NULL,
                    tipo        TEXT NOT NULL,
                    detalle     TEXT NOT NULL,
                    usuario     TEXT NOT NULL DEFAULT ''
                );
            """)

    def _con(self):
        con = sqlite3.connect(self.ruta)
        con.execute("PRAGMA foreign_keys = ON")
        return con

    # ---------------- migración desde la versión con JSON ----------------
    def migrar_json(self, ruta_json):
        """Importa indice.json una sola vez y lo renombra. Devuelve cuántos archivos importó."""
        if not os.path.exists(ruta_json):
            return 0
        with open(ruta_json, encoding="utf-8") as f:
            datos = json.load(f)
        with self._con() as con:
            for nodo_id, direccion in datos.get("nodos", {}).items():
                con.execute("INSERT OR IGNORE INTO nodos VALUES (?, ?, ?)", (nodo_id, direccion, _ahora()))
            for nombre, nodos in datos.get("archivos", {}).items():
                con.execute("INSERT OR IGNORE INTO archivos (nombre, subido) VALUES (?, ?)", (nombre, _ahora()))
                con.executemany("INSERT OR IGNORE INTO replicas VALUES (?, ?)", [(nombre, n) for n in nodos])
        os.replace(ruta_json, ruta_json + ".migrado")
        return len(datos.get("archivos", {}))

    # ---------------- nodos ----------------
    def guardar_nodo(self, nodo_id, direccion):
        with self._con() as con:
            con.execute("INSERT INTO nodos VALUES (?, ?, ?) "
                        "ON CONFLICT(id) DO UPDATE SET direccion = excluded.direccion",
                        (nodo_id, direccion, _ahora()))

    def nodos(self):
        with self._con() as con:
            return dict(con.execute("SELECT id, direccion FROM nodos"))

    # ---------------- archivos y réplicas ----------------
    def registrar_archivo(self, nombre, nodos, tamano, sha256):
        """Guarda (o reemplaza) un archivo y sus réplicas en una sola transacción."""
        with self._con() as con:
            con.execute("INSERT INTO archivos VALUES (?, ?, ?, ?) ON CONFLICT(nombre) DO UPDATE SET "
                        "tamano = excluded.tamano, sha256 = excluded.sha256, subido = excluded.subido",
                        (nombre, tamano, sha256, _ahora()))
            con.execute("DELETE FROM replicas WHERE archivo = ?", (nombre,))
            con.executemany("INSERT INTO replicas VALUES (?, ?)", [(nombre, n) for n in nodos])

    def eliminar_archivo(self, nombre):
        with self._con() as con:
            con.execute("DELETE FROM archivos WHERE nombre = ?", (nombre,))   # borra sus réplicas

    def replicas(self, nombre):
        """Nodos con réplica del archivo, o None si el archivo no existe."""
        with self._con() as con:
            if not con.execute("SELECT 1 FROM archivos WHERE nombre = ?", (nombre,)).fetchone():
                return None
            return [n for (n,) in con.execute(
                "SELECT nodo FROM replicas WHERE archivo = ? ORDER BY rowid", (nombre,))]

    def archivos(self):
        """Diccionario nombre -> lista de nodos con réplica."""
        resultado = {}
        with self._con() as con:
            for (nombre,) in con.execute("SELECT nombre FROM archivos ORDER BY subido, nombre"):
                resultado[nombre] = []
            for nombre, nodo in con.execute("SELECT archivo, nodo FROM replicas ORDER BY rowid"):
                resultado.setdefault(nombre, []).append(nodo)
        return resultado

    def carga(self):
        """Cantidad de archivos que tiene cada nodo."""
        with self._con() as con:
            return dict(con.execute("SELECT nodo, COUNT(*) FROM replicas GROUP BY nodo"))

    # ---------------- bitácora de eventos ----------------
    def evento(self, tipo, detalle, usuario=""):
        """Guarda un evento y lo devuelve con su id y fecha."""
        fecha = _ahora()
        with self._con() as con:
            cursor = con.execute(
                "INSERT INTO eventos (fecha, tipo, detalle, usuario) VALUES (?, ?, ?, ?)",
                (fecha, tipo, detalle, usuario))
        return {"id": cursor.lastrowid, "fecha": fecha, "tipo": tipo, "detalle": detalle,
                "usuario": usuario}
    
    def eventos(self, limite=50):
        """Los eventos más recientes, del más nuevo al más antiguo."""
        with self._con() as con:
            con.row_factory = sqlite3.Row
            return [dict(f) for f in con.execute(
                "SELECT id, fecha, tipo, detalle, usuario FROM eventos ORDER BY id DESC LIMIT ?",
                (limite,))] 