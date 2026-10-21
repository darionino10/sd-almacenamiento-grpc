"""Inicia todo el sistema con un solo comando.

Levanta, en orden, el coordinador, los nodos y el gateway web, y muestra los
registros de todos en esta terminal, cada uno con su color.

Uso:
    python iniciar.py                # coordinador + 3 nodos + gateway
    python iniciar.py --nodos 4      # cuatro nodos
    python iniciar.py --sin-web      # sin el gateway web
    python iniciar.py --detalle      # muestra también los pings de salud

Órdenes disponibles mientras corre (escribirlas y presionar Enter):
    parar nodo2      detiene un nodo (simula una caída)
    iniciar nodo2    vuelve a levantar un nodo detenido
    estado           muestra qué procesos están corriendo
    salir            detiene todo (también con Ctrl+C)
"""
import argparse
import os
import socket
import subprocess
import sys
import threading
import time

PUERTO_COORDINADOR = 50050
PUERTO_BASE_NODOS = 50051
PUERTO_WEB = 8000

COLORES = ["\033[95m", "\033[96m", "\033[92m", "\033[93m", "\033[94m", "\033[91m"]
RESET = "\033[0m"

procesos = {}            # nombre -> subprocess.Popen
comandos = {}            # nombre -> lista con el comando para (re)iniciarlo
colores = {}
candado_salida = threading.Lock()
mostrar_pings = False    # los pings de salud se ocultan para no llenar la terminal


def imprimir(nombre, texto):
    with candado_salida:
        print(f"{colores.get(nombre, '')}{nombre:<12}{RESET}| {texto}", flush=True)


def reenviar_salida(nombre, proceso):
    """Lee la salida de un proceso y la muestra con su nombre y color."""
    for linea in iter(proceso.stdout.readline, ""):
        if not mostrar_pings and "Ping recibido desde coordinador" in linea:
            continue
        imprimir(nombre, linea.rstrip())
    imprimir(nombre, f"(proceso terminado, código {proceso.wait()})")


def lanzar(nombre):
    entorno = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
    proceso = subprocess.Popen(comandos[nombre], stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                               errors="replace", env=entorno)
    procesos[nombre] = proceso
    threading.Thread(target=reenviar_salida, args=(nombre, proceso), daemon=True).start()


def esperar_puerto(puerto, segundos=10):
    """Espera a que un servidor acepte conexiones en el puerto indicado."""
    limite = time.time() + segundos
    while time.time() < limite:
        with socket.socket() as s:
            if s.connect_ex(("localhost", puerto)) == 0:
                return True
        time.sleep(0.2)
    return False


def detener(nombre):
    proceso = procesos.get(nombre)
    if proceso and proceso.poll() is None:
        proceso.terminate()
        try:
            proceso.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proceso.kill()


def detener_todo():
    # Primero el gateway y los nodos, al final el coordinador.
    for nombre in reversed(list(procesos)):
        detener(nombre)


def atender_ordenes():
    """Lee órdenes escritas en la terminal mientras el sistema corre."""
    for linea in sys.stdin:
        partes = linea.strip().split()
        if not partes:
            continue
        orden, objetivo = partes[0].lower(), (partes[1] if len(partes) > 1 else "")
        if orden == "salir":
            break
        elif orden == "estado":
            for nombre, proceso in procesos.items():
                estado = "corriendo" if proceso.poll() is None else "detenido"
                imprimir("sistema", f"{nombre}: {estado}")
        elif orden in ("parar", "iniciar") and objetivo in comandos:
            if orden == "parar":
                imprimir("sistema", f"Deteniendo {objetivo}…")
                detener(objetivo)
            elif procesos[objetivo].poll() is None:
                imprimir("sistema", f"{objetivo} ya está corriendo")
            else:
                imprimir("sistema", f"Iniciando {objetivo}…")
                lanzar(objetivo)
        else:
            imprimir("sistema", "Órdenes: parar <nombre>, iniciar <nombre>, estado, salir")


def main():
    parser = argparse.ArgumentParser(description="Inicia el sistema completo")
    parser.add_argument("--nodos", type=int, default=3, help="cantidad de nodos")
    parser.add_argument("--sin-web", action="store_true", help="no iniciar el gateway web")
    parser.add_argument("--detalle", action="store_true",
                        help="mostrar también los pings de salud del coordinador")
    args = parser.parse_args()
    global mostrar_pings
    mostrar_pings = args.detalle

    if os.name == "nt":
        os.system("")  # activa los colores ANSI en la consola de Windows

    py = sys.executable
    comandos["coordinador"] = [py, "-u", "coordinador.py", "--puerto", str(PUERTO_COORDINADOR)]
    for i in range(1, args.nodos + 1):
        comandos[f"nodo{i}"] = [py, "-u", "nodo.py", "--id", f"nodo{i}",
                                "--puerto", str(PUERTO_BASE_NODOS + i - 1)]
    if not args.sin_web:
        comandos["gateway"] = [py, "-m", "uvicorn", "gateway:app", "--port", str(PUERTO_WEB)]
    for i, nombre in enumerate(comandos):
        colores[nombre] = COLORES[i % len(COLORES)]
    colores["sistema"] = "\033[1m"

    try:
        lanzar("coordinador")
        if not esperar_puerto(PUERTO_COORDINADOR):
            imprimir("sistema", "El coordinador no arrancó; revisa su registro")
            return
        for nombre in comandos:
            if nombre.startswith("nodo"):
                lanzar(nombre)
        if not args.sin_web:
            esperar_puerto(PUERTO_BASE_NODOS)
            lanzar("gateway")
            if esperar_puerto(PUERTO_WEB):
                imprimir("sistema", f"Panel web en http://localhost:{PUERTO_WEB}")
        imprimir("sistema", "Sistema iniciado. Órdenes: parar <nodo>, iniciar <nodo>, "
                            "estado, salir (o Ctrl+C)")
        atender_ordenes()
    except KeyboardInterrupt:
        pass
    finally:
        imprimir("sistema", "Deteniendo todo…")
        detener_todo()
        time.sleep(0.3)
        imprimir("sistema", "Listo")


if __name__ == "__main__":
    main()