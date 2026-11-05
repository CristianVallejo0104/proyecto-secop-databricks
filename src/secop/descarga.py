"""Descarga de SECOP II desde la API Socrata de datos.gov.co.

La API usa parámetros que empiezan con ``$``:
``$where`` (filtro), ``$select`` (columnas), ``$order`` (orden estable para
paginar), ``$limit`` y ``$offset`` (tamaño y posición de la página).

Diseño:
- **Por mes:** si la API se cae a mitad, solo se repite un mes y no todo el año.
- **Reanudable:** cada página se guarda como archivo; al terminar un mes se crea
  un archivo vacío ``mes_MM.listo``. Al volver a ejecutar se salta lo que existe.
- **Reintentos con espera creciente:** 5 s, 10 s, 20 s... ante errores de red,
  429 (demasiadas peticiones) o 5xx (error del servidor).
- **Sin datos personales:** ``$select`` pide solo las columnas permitidas.

La función ``obtener`` (que hace el GET) se puede reemplazar en las pruebas, así
se prueba toda la lógica sin conectarse a internet.
"""

import math
import os
import time
from collections.abc import Callable

from secop import config
from secop.esquema import COLUMNAS_BRONCE

URL_JSON = config.URL_BASE_API + ".json"
URL_CSV = config.URL_BASE_API + ".csv"


def _obtener_con_requests(url: str, params: dict):
    """GET real a la API (timeout = (conexión, lectura) en segundos)."""
    import requests

    return requests.get(url, params=params, timeout=(30, 300))


def pedir_con_reintentos(
    url: str,
    params: dict,
    intentos: int = 6,
    espera: float = 5.0,
    obtener: Callable = _obtener_con_requests,
    dormir: Callable[[float], None] = time.sleep,
):
    """GET con reintentos y espera exponencial (espera, 2·espera, 4·espera...).

    Un 4xx distinto de 429 (petición mal formada) NO se reintenta: repetirlo no
    lo arregla, así que se lanza el error de inmediato.
    """
    for intento in range(intentos):
        try:
            respuesta = obtener(url, params)
            if respuesta.status_code == 200:
                return respuesta
            if respuesta.status_code != 429 and respuesta.status_code < 500:
                raise RuntimeError(f"HTTP {respuesta.status_code}: petición rechazada ({url})")
            motivo = f"HTTP {respuesta.status_code}"
        except (ConnectionError, TimeoutError, OSError) as error:
            motivo = type(error).__name__
        pausa = espera * 2**intento
        print(f"  intento {intento + 1}/{intentos} falló ({motivo}); espero {pausa:.0f} s")
        dormir(pausa)
    raise RuntimeError(f"La API no respondió después de {intentos} intentos: {url}")


def filtro_mes(anio: int, mes: int, columna: str = config.COLUMNA_FECHA_DESCARGA) -> str:
    """$where de los contratos firmados en un mes: [día 1, día 1 del mes siguiente)."""
    sig_anio, sig_mes = (anio + 1, 1) if mes == 12 else (anio, mes + 1)
    return (
        f"{columna} >= '{anio}-{mes:02d}-01T00:00:00' AND "
        f"{columna} < '{sig_anio}-{sig_mes:02d}-01T00:00:00'"
    )


def filtro_anio(anio: int, columna: str = config.COLUMNA_FECHA_DESCARGA) -> str:
    """$where de los contratos firmados en un año."""
    return f"{columna} >= '{anio}-01-01T00:00:00' AND {columna} < '{anio + 1}-01-01T00:00:00'"


def conteo_servidor(where: str, obtener: Callable = _obtener_con_requests) -> int:
    """Cuántas filas tiene el servidor para un filtro (sin descargarlas)."""
    respuesta = pedir_con_reintentos(
        URL_JSON, {"$select": "count(*) AS n", "$where": where}, obtener=obtener
    )
    return int(respuesta.json()[0]["n"])


def descargar_mes(
    anio: int,
    mes: int,
    obtener: Callable = _obtener_con_requests,
    tamano_pagina: int = config.TAMANO_PAGINA,
) -> int:
    """Descarga un mes en páginas CSV. Devuelve cuántas páginas descargó ahora.

    Salta el mes si existe ``mes_MM.listo`` y salta cada página que ya exista.
    """
    carpeta = config.ruta_anio(anio)
    os.makedirs(carpeta, exist_ok=True)
    marcador = os.path.join(carpeta, f"mes_{mes:02d}.listo")
    if os.path.exists(marcador):
        print(f"{anio}-{mes:02d}: ya estaba completo")
        return 0
    n = conteo_servidor(filtro_mes(anio, mes), obtener=obtener)
    paginas = math.ceil(n / tamano_pagina)
    print(f"{anio}-{mes:02d}: {n} filas en el servidor -> {paginas} páginas")
    descargadas = 0
    for p in range(paginas):
        destino = os.path.join(carpeta, f"mes_{mes:02d}_parte_{p:03d}.csv")
        if os.path.exists(destino):
            continue  # reanudación: esta página ya se descargó
        params = {
            "$select": ",".join(COLUMNAS_BRONCE),  # nunca pedimos columnas personales
            "$where": filtro_mes(anio, mes),
            "$order": ":id",  # orden estable: sin él, las páginas podrían repetir filas
            "$limit": tamano_pagina,
            "$offset": p * tamano_pagina,
        }
        respuesta = pedir_con_reintentos(URL_CSV, params, obtener=obtener)
        # Primero a .tmp y luego se renombra: si el proceso muere a mitad de la
        # escritura, no queda un CSV incompleto con nombre válido.
        with open(destino + ".tmp", "wb") as f:
            f.write(respuesta.content)
        os.replace(destino + ".tmp", destino)
        descargadas += 1
        print(f"  página {p + 1}/{paginas} guardada")
    open(marcador, "w").close()  # archivo vacío = mes terminado
    return descargadas
