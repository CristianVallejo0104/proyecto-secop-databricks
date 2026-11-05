# Imagen reproducible para validar el esquema con datos sintéticos.
# python:3.12-slim = Python 3.12 sobre Debian mínimo (imagen pequeña).
# Para fijar la imagen exacta byte a byte se puede usar su digest:
#   FROM python:3.12-slim@sha256:<digest>
FROM python:3.12-slim

# No generar archivos .pyc y mostrar la salida de print de inmediato.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Se copia solo lo necesario (ver .dockerignore): código y script, nunca datos.
COPY src/ src/
COPY scripts/ scripts/

# El validador usa solo la biblioteca estándar: no hay dependencias que instalar,
# así la imagen no cambia aunque PyPI publique versiones nuevas.
# Usuario sin privilegios: buena práctica de seguridad.
RUN useradd --create-home secop
USER secop

CMD ["python", "scripts/validar_esquema.py"]
