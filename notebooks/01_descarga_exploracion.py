# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Descarga y exploración de SECOP II
# MAGIC
# MAGIC **Qué hace:** descarga los contratos de la API de datos.gov.co por año y mes, los
# MAGIC guarda como CSV en el Volume `/Volumes/workspace/secop/raw/contratos/anio=AAAA/`,
# MAGIC compara cuántas filas tenemos contra cuántas dice el servidor y explora el esquema.
# MAGIC
# MAGIC **Por qué así:**
# MAGIC - **Por mes:** un año completo son cientos de miles de filas; si la API se cae a mitad,
# MAGIC   solo se repite un mes y no todo el año.
# MAGIC - **Reanudable:** cada página se guarda como archivo; al terminar un mes se crea un archivo
# MAGIC   vacío `mes_MM.listo`. Si el notebook se interrumpe, al volver a ejecutarlo salta lo que
# MAGIC   ya existe.
# MAGIC - **Reintentos con espera creciente:** la API a veces responde lento o con error 5xx/429.
# MAGIC   Se espera 5 s, 10 s, 20 s... antes de reintentar (no saturamos el servidor).
# MAGIC - **Sin datos personales:** se pide a la API solo la lista de columnas permitidas
# MAGIC   (`$select`), así nombres, documentos y cuentas bancarias nunca llegan a nuestro almacenamiento.
# MAGIC
# MAGIC Este notebook es autocontenido: no depende de variables de otros notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importar el código del repositorio (`src/secop`)
# MAGIC
# MAGIC En un **Git folder** de Databricks el directorio de trabajo es la carpeta del notebook
# MAGIC (`.../notebooks`). Subimos carpetas hasta encontrar `src/secop` y añadimos `src` a
# MAGIC `sys.path`, la lista de carpetas donde Python busca módulos.
# MAGIC
# MAGIC **Si el import falla:** escribe en el widget `ruta_repo` la ruta del repo, por ejemplo
# MAGIC `/Workspace/Users/tu_correo/proyecto-secop-databricks`. Si cambias código en `src/`,
# MAGIC ejecuta `dbutils.library.restartPython()` para que Python lo vuelva a cargar.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("ruta_repo", "", "Ruta del repo (vacío = automática)")


def buscar_raiz_repo() -> str:
    """Devuelve la carpeta raíz del repo (la que contiene src/secop)."""
    manual = dbutils.widgets.get("ruta_repo").strip()
    if manual:
        return manual
    actual = os.getcwd()
    while actual not in ("/", ""):
        if os.path.isdir(os.path.join(actual, "src", "secop")):
            return actual
        actual = os.path.dirname(actual)
    raise RuntimeError("No encontré src/secop. Escribe la ruta del repo en el widget 'ruta_repo'.")


RAIZ_REPO = buscar_raiz_repo()
if os.path.join(RAIZ_REPO, "src") not in sys.path:
    sys.path.insert(0, os.path.join(RAIZ_REPO, "src"))

import secop
from secop import config
from secop.esquema import COLUMNAS_BRONCE
from secop.validacion import columnas_personales_presentes, comparar_columnas

print("Código secop versión", secop.__version__, "desde", RAIZ_REPO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parámetros
# MAGIC - `anios`: años a descargar, separados por coma.
# MAGIC - `modo`: `descargar` (descarga lo que falte y verifica) o `solo_verificar` (no descarga).

# COMMAND ----------

dbutils.widgets.text("anios", "2019,2020", "Años")
dbutils.widgets.dropdown("modo", "solo_verificar", ["descargar", "solo_verificar"], "Modo")

ANIOS = [int(a) for a in dbutils.widgets.get("anios").split(",") if a.strip()]
MODO = dbutils.widgets.get("modo")
print("Años:", ANIOS, "| modo:", MODO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funciones de descarga
# MAGIC
# MAGIC La API Socrata usa parámetros que empiezan con `$`:
# MAGIC `$where` (filtro), `$select` (columnas), `$order` (orden estable para paginar),
# MAGIC `$limit` y `$offset` (tamaño y posición de la página).

# COMMAND ----------

import math
import time

import requests

URL_JSON = config.URL_BASE_API + ".json"
URL_CSV = config.URL_BASE_API + ".csv"
COL_FECHA = config.COLUMNA_FECHA_DESCARGA


def pedir_con_reintentos(url: str, params: dict, intentos: int = 6, espera: float = 5.0):
    """GET con reintentos y espera exponencial (5, 10, 20, 40... segundos).

    Reintenta ante errores de red, 429 (demasiadas peticiones) y 5xx (error del
    servidor). Un 4xx distinto (petición mal formada) no se reintenta: repetirlo
    no lo arregla.
    """
    for intento in range(intentos):
        try:
            # timeout=(conexión, lectura): la lectura de una página grande puede tardar.
            r = requests.get(url, params=params, timeout=(30, 300))
            if r.status_code == 200:
                return r
            if r.status_code != 429 and r.status_code < 500:
                r.raise_for_status()
            motivo = f"HTTP {r.status_code}"
        except (requests.ConnectionError, requests.Timeout) as e:
            motivo = type(e).__name__
        pausa = espera * 2**intento
        print(f"  intento {intento + 1}/{intentos} falló ({motivo}); espero {pausa:.0f} s")
        time.sleep(pausa)
    raise RuntimeError(f"La API no respondió después de {intentos} intentos: {url}")


def filtro_mes(anio: int, mes: int) -> str:
    """Filtro $where para los contratos firmados en un mes: [inicio, inicio del mes siguiente)."""
    sig_anio, sig_mes = (anio + 1, 1) if mes == 12 else (anio, mes + 1)
    return (
        f"{COL_FECHA} >= '{anio}-{mes:02d}-01T00:00:00' AND "
        f"{COL_FECHA} < '{sig_anio}-{sig_mes:02d}-01T00:00:00'"
    )


def filtro_anio(anio: int) -> str:
    return f"{COL_FECHA} >= '{anio}-01-01T00:00:00' AND {COL_FECHA} < '{anio + 1}-01-01T00:00:00'"


def conteo_servidor(where: str) -> int:
    """Cuántas filas tiene el servidor para un filtro (sin descargarlas)."""
    r = pedir_con_reintentos(URL_JSON, {"$select": "count(*) AS n", "$where": where})
    return int(r.json()[0]["n"])


def descargar_mes(anio: int, mes: int) -> None:
    """Descarga un mes en páginas; salta las páginas y meses que ya existen."""
    carpeta = config.ruta_anio(anio)
    os.makedirs(carpeta, exist_ok=True)
    marcador = f"{carpeta}/mes_{mes:02d}.listo"
    if os.path.exists(marcador):
        print(f"{anio}-{mes:02d}: ya estaba completo")
        return
    n = conteo_servidor(filtro_mes(anio, mes))
    paginas = math.ceil(n / config.TAMANO_PAGINA)
    print(f"{anio}-{mes:02d}: {n} filas en el servidor -> {paginas} páginas")
    for p in range(paginas):
        destino = f"{carpeta}/mes_{mes:02d}_parte_{p:03d}.csv"
        if os.path.exists(destino):
            continue  # reanudación: esta página ya se descargó
        params = {
            "$select": ",".join(COLUMNAS_BRONCE),  # nunca pedimos columnas personales
            "$where": filtro_mes(anio, mes),
            "$order": ":id",  # orden estable: sin él, las páginas podrían repetir filas
            "$limit": config.TAMANO_PAGINA,
            "$offset": p * config.TAMANO_PAGINA,
        }
        r = pedir_con_reintentos(URL_CSV, params)
        # Se escribe primero a .tmp y luego se renombra: si el proceso muere a
        # mitad de escritura, no queda un CSV incompleto con nombre válido.
        with open(destino + ".tmp", "wb") as f:
            f.write(r.content)
        os.replace(destino + ".tmp", destino)
        print(f"  página {p + 1}/{paginas} guardada")
    open(marcador, "w").close()  # archivo vacío = mes terminado


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Descargar (solo si `modo = descargar`)
# MAGIC Los meses con `mes_MM.listo` se saltan, así que 2019 y 2020 (ya completos) no se repiten.

# COMMAND ----------

if MODO == "descargar":
    for anio in ANIOS:
        for mes in range(1, 13):
            descargar_mes(anio, mes)
else:
    print("Modo solo_verificar: no se descarga nada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar: filas locales vs. servidor
# MAGIC
# MAGIC Leemos los CSV con Spark. Opciones importantes:
# MAGIC - `pathGlobFilter="*.csv"`: ignora los marcadores `.listo` y archivos `.tmp`.
# MAGIC - `multiLine=True` y `escape='"'`: algunas descripciones tienen saltos de línea y comillas
# MAGIC   dentro del texto; sin esto una fila se partiría en varias.
# MAGIC - Sin `inferSchema`: todo se lee como texto (los tipos se corrigen en plata).
# MAGIC
# MAGIC `count()` es una **acción**: es el momento en que Spark realmente lee los archivos.
# MAGIC Valores de referencia de la descarga previa: 2019 → 142.090 filas; 2020 → 356.623.

# COMMAND ----------


def leer_csv_anio(anio: int):
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("pathGlobFilter", "*.csv")
        .csv(config.ruta_anio(anio))
    )


filas = []
for anio in ANIOS:
    n_local = leer_csv_anio(anio).count()
    n_servidor = conteo_servidor(filtro_anio(anio))
    filas.append((anio, n_local, n_servidor, n_local - n_servidor))

display(
    spark.createDataFrame(
        filas, "anio int, filas_locales long, filas_servidor long, diferencia long"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Una diferencia pequeña puede deberse a contratos actualizados en el servidor después de
# MAGIC la descarga. Una diferencia grande indica un mes incompleto: borra su `mes_MM.listo`
# MAGIC y sus partes, y vuelve a ejecutar en modo `descargar`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Exploración del esquema
# MAGIC Revisamos que lleguen las columnas esperadas y que no haya columnas personales en lo que
# MAGIC vamos a mostrar. Solo se exploran columnas permitidas.

# COMMAND ----------

anio_explorar = ANIOS[0]
crudo = leer_csv_anio(anio_explorar)
resultado = comparar_columnas(crudo.columns, COLUMNAS_BRONCE)
print("Columnas leídas:", len(crudo.columns))
print("¿Están todas las permitidas?", resultado["valido"], "| faltantes:", resultado["faltantes"])
print(
    "Columnas personales en el CSV (descargas antiguas):",
    len(columnas_personales_presentes(crudo.columns)),
)

permitidas = [c for c in crudo.columns if c in COLUMNAS_BRONCE]
explorar = crudo.select(*permitidas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Porcentaje de vacíos por columna
# MAGIC Una sola agregación calcula todas las columnas a la vez (una pasada por los datos).

# COMMAND ----------

from pyspark.sql import functions as F

vacios = explorar.select(
    [F.round(F.avg(F.col(c).isNull().cast("int")) * 100, 1).alias(c) for c in permitidas]
)
# El resultado es una fila; stack la convierte en (columna, pct) para leerla mejor.
pares = ", ".join(f"'{c}', `{c}`" for c in permitidas)
display(
    vacios.select(F.expr(f"stack({len(permitidas)}, {pares}) AS (columna, pct_vacios)")).orderBy(
        F.desc("pct_vacios")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valores más frecuentes de columnas clave
# MAGIC Aquí se ven los problemas que se corrigen en plata: mayúsculas inconsistentes en el estado,
# MAGIC "No Definido", duración con formatos mezclados y localización compuesta.

# COMMAND ----------

for columna in [
    "estado_contrato",
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "habilita_pago_adelantado",
    "duraci_n_del_contrato",
    "localizaci_n",
]:
    print(columna)
    display(explorar.groupBy(columna).count().orderBy(F.desc("count")).limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribución del valor del contrato
# MAGIC `percentile_approx` calcula percentiles aproximados de forma distribuida (no ordena todo).

# COMMAND ----------

valor = F.col("valor_del_contrato").try_cast("double")
display(
    explorar.select(
        F.count(valor).alias("con_valor"),
        F.sum((valor <= 0).cast("int")).alias("valores_menores_o_iguales_a_0"),
        F.percentile_approx(valor, [0.5, 0.9, 0.99]).alias("p50_p90_p99"),
        F.max(valor).alias("maximo"),
    )
)
