# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Capa oro: tabla para el modelo
# MAGIC
# MAGIC **Qué hace:** une las tablas de plata en `workspace.secop.gold_contratos`: una fila por
# MAGIC contrato con la **etiqueta** y las **variables conocidas al firmar**.
# MAGIC
# MAGIC **¿Por qué desnormalizar si plata está en 3FN?** Plata está normalizada para *guardar*
# MAGIC bien (sin redundancia, con integridad). Oro se desnormaliza a propósito para *analizar*:
# MAGIC los descriptivos y el modelo necesitan todo en una fila, y repetir 6 joins en cada consulta
# MAGIC sería lento y propenso a errores. Normalizar para almacenar, desnormalizar para analizar.
# MAGIC
# MAGIC - **Etiqueta:** `etiqueta = 1` si `dias_adicionados > 0` (hubo prórroga), `0` si es 0.
# MAGIC   Si `dias_adicionados` está vacío, no sabemos qué pasó y la fila se excluye.
# MAGIC - **Variables:** tipo de contrato, modalidad y justificación (de `dim_proceso`), entidad
# MAGIC   (orden, sector, rama, departamento), proveedor (tipo de documento, grupo), pyme, categoría
# MAGIC   UNSPSC (familia y segmento, de `dim_familia_unspsc`), valor (recortado y en logaritmo),
# MAGIC   calendario de la firma y distancia firma-inicio.
# MAGIC - **Excluido por fuga:** estado, pagos, saldos, fechas de liquidación (nunca se unen).
# MAGIC - **Plazo (fin - inicio) y duración:** se guardan aparte porque NO sabemos si ya incluyen
# MAGIC   los días añadidos. El notebook 06 compara el modelo con y sin ellas.
# MAGIC - **Años 2025-2026:** quedan en la tabla con `etiqueta_completa = false` y no se usan
# MAGIC   para entrenar (aún pueden recibir prórrogas).

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
from secop.esquema import COLUMNAS_POSTERIORES_FIRMA
from secop.oro import construir_oro
from secop.validacion import exigir_sin_personales

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Construir oro
# MAGIC Las dimensiones pequeñas (entidad, categoría) se unen con **broadcast join**: Spark envía
# MAGIC una copia completa a cada ejecutor y así evita redistribuir (*shuffle*) la tabla grande.

# COMMAND ----------

oro = construir_oro(
    contrato=spark.table(config.TABLA_CONTRATO),
    dim_entidad=spark.table(config.TABLA_DIM_ENTIDAD),
    dim_proveedor=spark.table(config.TABLA_DIM_PROVEEDOR),
    dim_categoria=spark.table(config.TABLA_DIM_CATEGORIA),
    dim_familia=spark.table(config.TABLA_DIM_FAMILIA),
    dim_proceso=spark.table(config.TABLA_DIM_PROCESO),
    ejecucion=spark.table(config.TABLA_EJECUCION),
)

# Verificaciones ANTES de escribir: sin datos personales y sin columnas posteriores a la firma.
exigir_sin_personales(oro.columns)
fuga = [
    c
    for c in oro.columns
    if c in COLUMNAS_POSTERIORES_FIRMA or c in ("estado_contrato", "dias_adicionados")
]
assert not fuga, f"Columnas con fuga de información en oro: {fuga}"

(
    oro.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(config.TABLA_ORO)
)
print("escrita:", config.TABLA_ORO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificación
# MAGIC Filas y tasa de prórroga por año de firma.
# MAGIC Referencia de la exploración previa para 2019: **141.257 filas y 10,99 % con etiqueta 1**.
# MAGIC Si el resultado difiere, revisar los filtros de `construir_oro` (valor > 0, fecha de firma
# MAGIC y etiqueta conocidas) y anotar la diferencia en `docs/informe.md`.

# COMMAND ----------

gold = spark.table(config.TABLA_ORO)
display(
    gold.groupBy("anio_firma", "etiqueta_completa")
    .agg(
        F.count("*").alias("contratos"),
        F.sum("etiqueta").alias("prorrogados"),
        F.round(F.avg("etiqueta") * 100, 2).alias("tasa_pct"),
    )
    .orderBy("anio_firma")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Completitud de las variables
# MAGIC Porcentaje de vacíos en las variables numéricas (las categóricas nunca son nulas: un vacío
# MAGIC se convierte en la categoría "No Definido").

# COMMAND ----------

numericas = config.VARIABLES_NUMERICAS + config.VARIABLES_PLAZO
display(
    gold.select(
        [F.round(F.avg(F.col(c).isNull().cast("int")) * 100, 2).alias(c) for c in numericas]
    )
)
