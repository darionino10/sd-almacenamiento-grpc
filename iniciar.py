"""Inicia el sistema con un solo comando, según el papel de este equipo.

Equipo central (coordinador, nodos locales y gateway web):
    python iniciar.py                     # coordinador + nodo1..nodo3 + gateway
    python iniciar.py --nodos 2           # solo nodo1 y nodo2 en este equipo
    python iniciar.py --ids nodo1,nodo3   # nodos concretos (nodo2 corre en otro equipo)

Equipo que solo aporta un nodo:
    python iniciar.py --rol nodo --id nodo2 --coordinador 100.123.23.123

Opciones comunes:
    --ip 100.x.y.z    IP con la que este equipo se anuncia (por defecto se toma de
                      config_local.py o se detecta con Tailscale)
    --sin-web         no iniciar el gateway web
    --detalle         mostrar también los pings de salud y las consultas del panel

Órdenes mientras corre (escribirlas y presionar Enter):
    detener nodo2     detiene un proceso (simula una caída)
    iniciar nodo2     vuelve a levantar un proceso detenido
    estado            muestra qué procesos están corriendo
    salir             detiene todo (también con Ctrl+C)
"""
import argparse
import os
import socket
import subprocess
import sys
import threading
import time

import config

COLORES = ["\033[95m", "\033[96m", "\033[92m", "\033[93m", "\033[94m", "\033[91m"]
RESET = "\033[0m"

# Líneas repetitivas que se ocultan para no llenar la terminal
RUIDO = (
    "Ping recibido desde coordinador",   # chequeo de salud cada 5 s
    '"GET /api/nodos ',                  # el panel consulta cada 2 s
    '"GET /api/archivos ',
)

procesos = {}            # nombre -> subprocess.Popen
comandos = {}            # nombre -> lista con el comando para (re)iniciarlo
colores = {}
candado_salida = threading.Lock()
mostrar_todo = False


def imprimir(nombre, texto):
    with candado_salida:
        print(f"{colores.get(nombre, '')}{nombre:<12}{RESET}| {texto}", flush=True)


def reenviar_salida(nombre, proceso):
    """Lee la salida de un proceso y la muestra con su nombre y color."""
    for linea in iter(proceso.stdout.readline, ""):
        if not mostrar_todo and any(texto in linea for texto in RUIDO):
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


def esperar_puerto(host, puerto, segundos=10):
    """Espera a que un servidor acepte conexiones en host:puerto."""
    limite = time.time() + segundos
    while time.time() < limite:
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex((host, puerto)) == 0:
                return True
        time.sleep(0.2)
    return False


def ip_tailscale():
    """Devuelve la IP de Tailscale de este equipo, o None si no está disponible."""
    try:
        r = subprocess.run(["tailscale", "ip", "-4"], capture_output=True, text=True, timeout=5)
        lineas = r.stdout.strip().splitlines()
        return lineas[0] if r.returncode == 0 and lineas else None
    except (OSError, subprocess.TimeoutExpired):
        return None


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
        orden, objetivo = partes[0].lower(), (partes[1].lower() if len(partes) > 1 else "")
        if orden == "parar":
            orden = "detener"            # se acepta la orden anterior
        if orden == "salir":
            break
        elif orden == "estado":
            for nombre, proceso in procesos.items():
                estado = "corriendo" if proceso.poll() is None else "detenido"
                imprimir("sistema", f"{nombre}: {estado}")
        elif orden in ("detener", "iniciar") and objetivo in comandos:
            if orden == "detener":
                imprimir("sistema", f"Deteniendo {objetivo}…")
                detener(objetivo)
            elif procesos[objetivo].poll() is None:
                imprimir("sistema", f"{objetivo} ya está corriendo")
            else:
                imprimir("sistema", f"Iniciando {objetivo}…")
                lanzar(objetivo)
        else:
            imprimir("sistema", "Órdenes: detener <nombre>, iniciar <nombre>, estado, salir")


def comando_nodo(py, nodo_id, ip, coordinador):
    return [py, "-u", "nodo.py", "--id", nodo_id, "--puerto", str(config.puerto_de_nodo(nodo_id)),
            "--host", ip, "--coordinador", coordinador]


def main():
    global mostrar_todo
    parser = argparse.ArgumentParser(description="Inicia el sistema según el papel del equipo")
    parser.add_argument("--rol", choices=["central", "nodo"], default="central")
    parser.add_argument("--nodos", type=int, default=3,
                        help="(central) cantidad de nodos locales: nodo1..nodoN")
    parser.add_argument("--ids", default=None,
                        help="(central) nodos locales separados por coma, por ejemplo nodo1,nodo3")
    parser.add_argument("--id", help="(nodo) identificador del nodo, por ejemplo nodo2")
    parser.add_argument("--coordinador", default=None,
                        help="(nodo) IP o host del equipo central")
    parser.add_argument("--ip", default=None, help="IP con la que se anuncia este equipo")
    parser.add_argument("--sin-web", action="store_true", help="no iniciar el gateway web")
    parser.add_argument("--detalle", action="store_true",
                        help="mostrar también pings de salud y consultas del panel")
    args = parser.parse_args()
    mostrar_todo = args.detalle

    if os.name == "nt":
        os.system("")  # activa los colores ANSI en la consola de Windows
    colores["sistema"] = "\033[1m"

    # IP propia: argumento > config_local.py > Tailscale > localhost
    ip = args.ip or (config.IP_PROPIA if config.IP_PROPIA != "localhost" else None) \
        or ip_tailscale() or "localhost"
    py = sys.executable

    if args.rol == "central":
        coordinador = f"localhost:{config.PUERTO_COORDINADOR}"
        comandos["coordinador"] = [py, "-u", "coordinador.py",
                                   "--puerto", str(config.PUERTO_COORDINADOR)]
        ids = ([i.strip() for i in args.ids.split(",") if i.strip()] if args.ids
               else [f"nodo{i}" for i in range(1, args.nodos + 1)])
        for nodo_id in ids:
            comandos[nodo_id] = comando_nodo(py, nodo_id, ip, coordinador)
        if not args.sin_web:
            comandos["gateway"] = [py, "-m", "uvicorn", "gateway:app",
                                   "--host", config.HOST_WEB, "--port", str(config.PUERTO_WEB)]
    else:
        if not args.id:
            sys.exit("Con --rol nodo hay que indicar --id, por ejemplo --id nodo2")
        host_central = args.coordinador or config.COORDINADOR_HOST
        coordinador = f"{host_central}:{config.PUERTO_COORDINADOR}"
        comandos[args.id] = comando_nodo(py, args.id, ip, coordinador)

    for i, nombre in enumerate(comandos):
        colores[nombre] = COLORES[i % len(COLORES)]

    imprimir("sistema", f"Rol: {args.rol} · IP de este equipo: {ip} · coordinador: {coordinador}")
    if ip == "localhost":
        imprimir("sistema", "Aviso: no se detectó Tailscale; solo funcionará en este equipo")

    try:
        if args.rol == "central":
            lanzar("coordinador")
            if not esperar_puerto("localhost", config.PUERTO_COORDINADOR):
                imprimir("sistema", "El coordinador no arrancó; revisa su registro")
                return
            for nombre in comandos:
                if nombre.startswith("nodo"):
                    lanzar(nombre)
            if not args.sin_web:
                lanzar("gateway")
                if esperar_puerto("localhost", config.PUERTO_WEB):
                    imprimir("sistema", f"Panel web: http://localhost:{config.PUERTO_WEB} "
                                        f"(otros equipos: http://{ip}:{config.PUERTO_WEB})")
        else:
            host, puerto = coordinador.rsplit(":", 1)
            if not esperar_puerto(host, int(puerto), segundos=5):
                imprimir("sistema", f"Aviso: no se alcanza el coordinador en {coordinador}; "
                                    "el nodo arranca igual y funcionará de forma independiente")
            lanzar(args.id)
        imprimir("sistema", "Listo. Órdenes: detener <nombre>, iniciar <nombre>, estado, salir")
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