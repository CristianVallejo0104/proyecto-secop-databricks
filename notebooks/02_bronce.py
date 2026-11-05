# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Capa bronce: CSV → tabla Delta
# MAGIC
# MAGIC **Qué hace:** lee los CSV de cada año del Volume y los escribe en la tabla Delta
# MAGIC `workspace.secop.bronze_contratos`, particionada por `anio_carga`.
# MAGIC
# MAGIC **Reglas de bronce:**
# MAGIC 1. **Todo como texto (string).** Bronce es una copia fiel; si convirtiéramos tipos aquí y
# MAGIC    la conversión fallara, perderíamos el dato original. Los tipos se corrigen en plata.
# MAGIC 2. **Sin datos personales.** Las columnas personales se descartan antes de escribir
# MAGIC    (por si el CSV es una descarga antigua que las traía).
# MAGIC 3. **Recarga por año con `replaceWhere`.** Volver a cargar 2019 reemplaza SOLO la partición
# MAGIC    `anio_carga = 2019`; los demás años no se tocan. Así el notebook se puede repetir sin
# MAGIC    duplicar filas (es *idempotente*).
# MAGIC
# MAGIC **¿Por qué Delta y no CSV/Parquet?** Delta agrega un registro de transacciones
# MAGIC (`_delta_log`): escrituras atómicas (o todo o nada), esquema validado e historial de
# MAGIC versiones (*time travel*).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importar `src/secop` (ver explicación en el notebook 01)

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

from pyspark.sql import functions as F

from secop import config
from secop.bronce import preparar_bronce
from secop.validacion import encabezados_distintos, exigir_sin_personales

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parámetros
# MAGIC - `anios`: años a cargar.
# MAGIC - `recrear_tabla`: `si` borra la tabla y la crea de nuevo. Úsalo solo si la tabla existe
# MAGIC   con otro esquema o partición (por ejemplo, una versión anterior con 95 columnas).

# COMMAND ----------

dbutils.widgets.text("anios", "2019,2020", "Años")
dbutils.widgets.dropdown("recrear_tabla", "no", ["no", "si"], "Recrear tabla")

ANIOS = [int(a) for a in dbutils.widgets.get("anios").split(",") if a.strip()]
RECREAR = dbutils.widgets.get("recrear_tabla") == "si"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOGO}.{config.ESQUEMA}")
if RECREAR:
    spark.sql(f"DROP TABLE IF EXISTS {config.TABLA_BRONCE}")
    print("Tabla eliminada; se creará de nuevo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cargar cada año
# MAGIC
# MAGIC - Antes de leer, se verifica que todos los CSV del año tengan el **mismo encabezado**:
# MAGIC   Spark asigna columnas por posición usando el primer archivo, y una mezcla de formatos
# MAGIC   desalinearía los datos sin dar error.
# MAGIC - `_metadata.file_name` es una columna oculta que Spark ofrece al leer archivos:
# MAGIC   guardamos de qué archivo viene cada fila (trazabilidad).
# MAGIC - Leemos la carpeta del año directamente (`.../anio=2019`), no la carpeta padre. Si se
# MAGIC   leyera `.../contratos`, Spark descubriría la carpeta `anio=` como una columna de
# MAGIC   partición llamada `anio`, distinta de nuestra `anio_carga`.

# COMMAND ----------


def leer_csv_anio(anio: int):
    carpeta = config.ruta_anio(anio)
    rutas = sorted(os.path.join(carpeta, f) for f in os.listdir(carpeta) if f.endswith(".csv"))
    if not rutas:
        raise FileNotFoundError(f"No hay CSV en {carpeta}. Ejecuta primero el notebook 01.")
    grupos = encabezados_distintos(rutas)
    if len(grupos) > 1:
        detalle = {len(e.split(",")): len(a) for e, a in grupos.items()}
        raise ValueError(
            f"{anio}: los CSV tienen encabezados distintos ({detalle} columnas: archivos). "
            "Vuelve a descargar el año completo con un solo método."
        )
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("pathGlobFilter", "*.csv")
        .csv(carpeta)
        .withColumn("archivo_origen", F.col("_metadata.file_name"))
    )


for anio in ANIOS:
    bronce = preparar_bronce(leer_csv_anio(anio), anio)
    exigir_sin_personales(bronce.columns)  # doble verificación antes de escribir

    escritor = bronce.write.format("delta").mode("overwrite")
    if spark.catalog.tableExists(config.TABLA_BRONCE):
        # Reemplaza SOLO la partición de este año (los demás años quedan intactos).
        escritor = escritor.option("replaceWhere", f"anio_carga = {anio}")
    else:
        # Primera escritura: se crea la tabla particionada por año de carga.
        escritor = escritor.partitionBy("anio_carga")
    escritor.saveAsTable(config.TABLA_BRONCE)
    print(f"{anio}: partición escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificación
# MAGIC Filas por año (referencia de la descarga previa: 2019 → 142.090; 2020 → 356.623) y
# MAGIC confirmación de que la tabla no tiene columnas personales.

# COMMAND ----------

tabla = spark.table(config.TABLA_BRONCE)
exigir_sin_personales(tabla.columns)
print("Columnas en bronce:", len(tabla.columns))
display(tabla.groupBy("anio_carga").count().orderBy("anio_carga"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Historial de Delta (*time travel*)
# MAGIC Cada escritura crea una versión. Se puede consultar una versión anterior con
# MAGIC `SELECT * FROM tabla VERSION AS OF n`. Útil para auditar o recuperar una carga mala.

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {config.TABLA_BRONCE}").select(
        "version", "timestamp", "operation", "operationParameters"
    )
)
