# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Capa plata: modelo normalizado
# MAGIC
# MAGIC **Qué hace:** lee `bronze_contratos` y construye seis tablas limpias y relacionadas:
# MAGIC
# MAGIC | Tabla | Llave | Contenido |
# MAGIC |---|---|---|
# MAGIC | `dim_entidad` | `codigo_entidad` | Entidad compradora (un solo nombre por código) |
# MAGIC | `dim_proveedor` | `codigo_proveedor` | Código interno y tipo de documento. **Sin datos personales** |
# MAGIC | `dim_categoria` | `codigo_categoria` | Código UNSPSC, familia y segmento |
# MAGIC | `contrato` | `id_contrato` | Lo que se sabe **al firmar** |
# MAGIC | `contrato_fuente_recurso` | `id_contrato` + `fuente` | Fuentes de recursos en formato largo |
# MAGIC | `contrato_ejecucion` | `id_contrato` | Lo que pasa **después** de firmar (estado, pagos, días añadidos) |
# MAGIC
# MAGIC **¿Por qué normalizar?** Cada dato vive en un solo lugar (el nombre de una entidad no se
# MAGIC repite en 100.000 filas), y separar "al firmar" de "después" hace muy difícil usar por
# MAGIC error una variable con fuga de información.
# MAGIC
# MAGIC Toda la lógica está en `src/secop/plata.py` (probada con pytest). Aquí solo se lee,
# MAGIC se llama a las funciones y se escribe.

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

from secop import config, plata
from secop.validacion import exigir_sin_personales

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer bronce
# MAGIC `spark.table` no lee datos todavía: crea un **plan**. Spark es *perezoso* (lazy):
# MAGIC las transformaciones se acumulan y solo se ejecutan al llegar a una **acción**
# MAGIC (escribir, contar, mostrar).

# COMMAND ----------

bronce = spark.table(config.TABLA_BRONCE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Construir y escribir las tablas
# MAGIC
# MAGIC Cada tabla se escribe con `overwrite` (se reconstruye completa: es pequeña y así el
# MAGIC resultado no depende de ejecuciones anteriores) y `overwriteSchema` (si cambiamos una
# MAGIC columna, la tabla se actualiza).
# MAGIC
# MAGIC **Nota sobre serverless:** no usamos `cache()`/`persist()` (no están disponibles). Cada
# MAGIC tabla recalcula desde bronce; con ~500 mil filas es rápido. Si fuera lento, la solución
# MAGIC es materializar un paso intermedio como tabla Delta, no cachear.

# COMMAND ----------

TABLAS = {
    config.TABLA_DIM_ENTIDAD: plata.construir_dim_entidad(bronce),
    config.TABLA_DIM_PROVEEDOR: plata.construir_dim_proveedor(bronce),
    config.TABLA_DIM_CATEGORIA: plata.construir_dim_categoria(bronce),
    config.TABLA_CONTRATO: plata.construir_contrato(bronce),
    config.TABLA_FUENTE_RECURSO: plata.construir_fuente_recurso(bronce),
    config.TABLA_EJECUCION: plata.construir_ejecucion(bronce),
}

for nombre, df in TABLAS.items():
    exigir_sin_personales(df.columns)  # ninguna tabla de plata puede tener datos personales
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(nombre)
    )
    print("escrita:", nombre)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Integridad referencial
# MAGIC Cada llave foránea debe existir en su tabla padre. `left_anti` devuelve las filas del
# MAGIC hijo **sin** pareja en el padre: el conteo debe ser **0**. Si no lo es, el notebook se
# MAGIC detiene (mejor fallar aquí que entrenar con datos rotos).

# COMMAND ----------

t = {nombre.split(".")[-1]: spark.table(nombre) for nombre in TABLAS}
revisiones = [
    ("contrato", "dim_entidad", "codigo_entidad"),
    ("contrato", "dim_proveedor", "codigo_proveedor"),
    ("contrato", "dim_categoria", "codigo_categoria"),
    ("contrato_fuente_recurso", "contrato", "id_contrato"),
    ("contrato_ejecucion", "contrato", "id_contrato"),
]
resultados = []
for hijo, padre, llave in revisiones:
    huerfanas = plata.filas_huerfanas(t[hijo], t[padre], llave).count()
    resultados.append((hijo, padre, llave, huerfanas))

display(
    spark.createDataFrame(resultados, "hijo string, padre string, llave string, huerfanas long")
)
rotas = [r for r in resultados if r[3] != 0]
assert not rotas, f"Integridad referencial rota: {rotas}"
print("Integridad referencial: OK (0 huérfanas en todas las relaciones)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Comprobaciones de calidad
# MAGIC - Llaves únicas en dimensiones y en `contrato`.
# MAGIC - Entidades que tenían más de un nombre (en 2019 se observaron 8: nombre completo y sigla).

# COMMAND ----------

unicidad = []
for tabla_nombre, llave in [
    ("dim_entidad", "codigo_entidad"),
    ("dim_proveedor", "codigo_proveedor"),
    ("dim_categoria", "codigo_categoria"),
    ("contrato", "id_contrato"),
    ("contrato_ejecucion", "id_contrato"),
]:
    fila = (
        t[tabla_nombre]
        .agg(F.count("*").alias("filas"), F.countDistinct(llave).alias("llaves"))
        .first()
    )
    unicidad.append((tabla_nombre, fila["filas"], fila["llaves"]))
display(spark.createDataFrame(unicidad, "tabla string, filas long, llaves_distintas long"))
assert all(f == k for _, f, k in unicidad), "Hay llaves duplicadas"

display(
    t["dim_entidad"].filter("n_nombres > 1").select("codigo_entidad", "nombre_entidad", "n_nombres")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Vistazo a las limpiezas
# MAGIC Estado unificado (ya sin mayúsculas inconsistentes) y duración separada en número y unidad.

# COMMAND ----------

display(t["contrato_ejecucion"].groupBy("estado_contrato").count().orderBy(F.desc("count")))
display(
    t["contrato"]
    .groupBy("duracion_unidad")
    .agg(F.count("*").alias("contratos"), F.avg("duracion_dias_declarada").alias("dias_promedio"))
)
