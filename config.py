"""Configuración central del sistema de almacenamiento distribuido.

Todos los componentes toman de aquí puertos, réplicas, intervalos y plazos,
para no repetir valores en cada archivo.

Los valores propios de cada equipo (por ejemplo, su IP de Tailscale) van en
`config_local.py`, que no se sube a Git. Ejemplo de config_local.py:

    IP_PROPIA = "100.123.23.123"          # IP con la que este equipo se anuncia
    COORDINADOR_HOST = "100.123.23.123"   # dónde está el coordinador
"""

# ---------------- red ----------------
IP_PROPIA = "localhost"          # dirección con la que los nodos se anuncian
COORDINADOR_HOST = "localhost"   # equipo donde corre el coordinador
PUERTO_COORDINADOR = 50050
PUERTO_BASE_NODOS = 50051        # nodo1 = 50051, nodo2 = 50052, ...
HOST_WEB = "0.0.0.0"             # el panel escucha en todas las interfaces
PUERTO_WEB = 8000

# ---------------- replicación y salud ----------------
REPLICAS = 2                     # copias de cada archivo
INTERVALO_SALUD = 5              # segundos entre pings del coordinador a los nodos
PLAZO_PING = 2                   # segundos para considerar que un nodo no responde

# ---------------- transferencia ----------------
TAMANO_FRAGMENTO = 64 * 1024     # 64 KB por fragmento
PLAZO = 5                        # deadline de llamadas cortas (segundos)
PLAZO_ARCHIVO = 30               # deadline de subidas y descargas (segundos)

# ---------------- cuentas y sesiones (gateway) ----------------
CARPETA_GATEWAY = "datos_gateway"   # base de datos de usuarios y sesiones
DURACION_SESION_HORAS = 8           # tiempo que dura una sesión iniciada

# ---------------- valores propios de este equipo ----------------
try:
    from config_local import *   # noqa: F401,F403  (archivo opcional, fuera de Git)
except ImportError:
    pass

COORDINADOR = f"{COORDINADOR_HOST}:{PUERTO_COORDINADOR}"


def puerto_de_nodo(nodo_id):
    """nodo2 -> 50052. Si el id no termina en número, usa el puerto base."""
    numero = "".join(c for c in nodo_id if c.isdigit())
    return PUERTO_BASE_NODOS + int(numero) - 1 if numero else PUERTO_BASE_NODOS