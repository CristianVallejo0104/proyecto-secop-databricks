# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Análisis descriptivo de las prórrogas
# MAGIC
# MAGIC **Qué hace:** calcula la tasa de prórroga (porcentaje de contratos con días añadidos)
# MAGIC por tipo de contrato, modalidad, entidad, departamento, mes y año.
# MAGIC
# MAGIC **Cómo:** todo se calcula con agregaciones de Spark (`groupBy` + `agg`), que se ejecutan
# MAGIC de forma distribuida. Lo que se muestra con `display()` es el **resultado agregado**
# MAGIC (unas decenas de filas), nunca la tabla completa. No usamos `toPandas()` sobre `gold`:
# MAGIC traería cientos de miles de filas a la memoria del driver (8 GB en Free Edition).
# MAGIC
# MAGIC Solo se usan años con etiqueta completa (2025-2026 aún pueden recibir prórrogas y
# MAGIC sesgarían la tasa hacia abajo).
# MAGIC
# MAGIC **Resultados:** PENDIENTE (se anotan en `docs/informe.md` después de ejecutar).

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
from secop.descriptivo import tasa_por_grupo
from secop.modelado import solo_etiqueta_completa

dbutils.widgets.text("min_contratos", "100", "Mínimo de contratos por grupo")
MIN_CONTRATOS = int(dbutils.widgets.get("min_contratos"))

gold = solo_etiqueta_completa(spark.table(config.TABLA_ORO))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tasa global
# MAGIC Es la referencia para todo lo demás: un grupo "alto" es uno por encima de esta tasa.
# MAGIC También es la AUC-PR de un modelo que no sabe nada (ver notebook 06).

# COMMAND ----------

display(
    gold.agg(
        F.count("*").alias("contratos"),
        F.sum("etiqueta").alias("prorrogados"),
        F.round(F.avg("etiqueta") * 100, 2).alias("tasa_prorroga_pct"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Por año y mes de firma
# MAGIC ¿Hay estacionalidad? (por ejemplo, contratos firmados a fin de año que se prorrogan para
# MAGIC terminar en la vigencia siguiente).

# COMMAND ----------

display(tasa_por_grupo(gold, ["anio_firma"]).orderBy("anio_firma"))
display(tasa_por_grupo(gold, ["anio_firma", "mes_firma"]).orderBy("anio_firma", "mes_firma"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Por tipo de contrato y modalidad
# MAGIC `min_contratos` (widget) descarta grupos diminutos cuya tasa no sería confiable.

# COMMAND ----------

display(tasa_por_grupo(gold, ["tipo_de_contrato"], MIN_CONTRATOS))
display(tasa_por_grupo(gold, ["modalidad_de_contratacion"], MIN_CONTRATOS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Por departamento y por entidad
# MAGIC Para mostrar el nombre de la entidad se une el resultado agregado (pequeño) con
# MAGIC `dim_entidad`, usando broadcast.

# COMMAND ----------

display(tasa_por_grupo(gold, ["departamento_entidad"], MIN_CONTRATOS))

por_entidad = tasa_por_grupo(gold, ["codigo_entidad"], MIN_CONTRATOS)
entidades = spark.table(config.TABLA_DIM_ENTIDAD).select(
    "codigo_entidad", "nombre_entidad", "orden_entidad"
)
display(
    por_entidad.join(F.broadcast(entidades), "codigo_entidad", "left")
    .select("nombre_entidad", "orden_entidad", "contratos", "prorrogados", "tasa_prorroga_pct")
    .orderBy(F.desc("tasa_prorroga_pct"))
    .limit(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Otras variables al firmar
# MAGIC Cláusula de prórroga, inicio antes de la firma y segmento UNSPSC.

# COMMAND ----------

display(tasa_por_grupo(gold, ["puede_ser_prorrogado"]))
display(tasa_por_grupo(gold, ["inicio_antes_de_firma"]))
display(tasa_por_grupo(gold, ["segmento_unspsc"], MIN_CONTRATOS).limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Diagnóstico de fuga: plazo según la etiqueta
# MAGIC Si `fecha_fin` ya incluye los días añadidos, los contratos prorrogados tendrán un plazo
# MAGIC (fin − inicio) mucho mayor que su duración declarada. Comparamos medianas por clase.
# MAGIC Una diferencia grande entre `plazo_dias` y `duracion_dias_declarada` **solo** en la clase 1
# MAGIC sería evidencia de fuga.

# COMMAND ----------

display(
    gold.groupBy("etiqueta").agg(
        F.count("*").alias("contratos"),
        F.percentile_approx("plazo_dias", 0.5).alias("mediana_plazo_dias"),
        F.percentile_approx("duracion_dias_declarada", 0.5).alias("mediana_duracion_declarada"),
        F.percentile_approx(F.col("plazo_dias") - F.col("duracion_dias_declarada"), 0.5).alias(
            "mediana_diferencia"
        ),
    )
)
