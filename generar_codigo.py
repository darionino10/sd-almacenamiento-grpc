"""Genera el código Python a partir del contrato protos/almacenamiento.proto."""
from grpc_tools import protoc

resultado = protoc.main([
    "grpc_tools.protoc",
    "-Iprotos",
    "--python_out=.",
    "--grpc_python_out=.",
    "protos/almacenamiento.proto",
])
if resultado != 0:
    raise SystemExit("Error al generar el código desde almacenamiento.proto")
print("Generados: almacenamiento_pb2.py y almacenamiento_pb2_grpc.py")