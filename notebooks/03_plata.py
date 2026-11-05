# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Capa plata: modelo relacional en tercera forma normal (3FN)
# MAGIC
# MAGIC **Qué hace:** lee `bronze_contratos` y construye **10 tablas** limpias y relacionadas,
# MAGIC verifica en los datos las dependencias funcionales en que se basa el diseño, y comprueba
# MAGIC llaves primarias e integridad referencial.
# MAGIC
# MAGIC | Tabla | Llave primaria | Contenido |
# MAGIC |---|---|---|
# MAGIC | `dim_departamento` | `departamento` | Departamento y su país |
# MAGIC | `dim_municipio` | `departamento` + `municipio` | Municipios (llave compuesta: hay nombres repetidos entre departamentos) |
# MAGIC | `dim_entidad` | `codigo_entidad` | Entidad compradora; ubicación como FK (departamento, municipio) |
# MAGIC | `dim_proveedor` | `codigo_proveedor` | Código interno, tipo de documento y si es grupo. **Sin datos personales** |
# MAGIC | `dim_familia_unspsc` | `familia_unspsc` | Familia UNSPSC y su segmento |
# MAGIC | `dim_categoria` | `codigo_categoria` | Código UNSPSC y FK a su familia |
# MAGIC | `dim_proceso` | `proceso_de_compra` | Modalidad y justificación (son del proceso, no del contrato) |
# MAGIC | `contrato` | `id_contrato` | Lo que se sabe **al firmar** |
# MAGIC | `contrato_fuente_recurso` | `id_contrato` + `fuente` | Fuentes de recursos en formato largo |
# MAGIC | `contrato_ejecucion` | `id_contrato` | Lo que pasa **después** de firmar (estado, pagos, días añadidos) |
# MAGIC
# MAGIC **Formas normales:**
# MAGIC - **1FN** (valores atómicos, sin grupos repetidos): la localización "País, Departamento,
# MAGIC   Ciudad" se separa; "6 Mes(es)" pasa a número y unidad; las 6 columnas de fuentes de
# MAGIC   recursos pasan a filas.
# MAGIC - **2FN** (con llave compuesta, todo depende de la llave completa): en
# MAGIC   `contrato_fuente_recurso`, el valor depende del contrato **y** de la fuente; `dim_municipio`
# MAGIC   no tiene atributos que dependan solo del departamento (el país está en `dim_departamento`).
# MAGIC - **3FN** (sin dependencias transitivas): la modalidad depende del proceso, no del contrato;
# MAGIC   el segmento depende de la familia, no del código; el país depende del departamento.
# MAGIC   Tampoco se guardan datos calculables (los días de duración se calculan en oro).
# MAGIC
# MAGIC Toda la lógica está en `src/secop/plata.py` (probada con pytest). Aquí solo se lee,
# MAGIC se llama a las funciones, se escribe y se verifica.

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
# MAGIC ## 2. Dependencias funcionales: ¿los datos cumplen lo que supone el diseño?
# MAGIC
# MAGIC Normalizar se basa en **dependencias funcionales**: "X → Y" significa que cada valor de X
# MAGIC tiene un único valor de Y (por ejemplo, cada proceso de compra tiene una sola modalidad).
# MAGIC Aquí se cuenta, en los datos reales, cuántos valores de X aparecen con más de un Y.
# MAGIC
# MAGIC - **0 violaciones:** la dependencia se cumple y la tabla está bien separada.
# MAGIC - **Más de 0:** hay datos inconsistentes en la fuente. La dimensión los resuelve con una
# MAGIC   regla explícita (el nombre más largo o el valor más reciente). Anotar el número en
# MAGIC   `docs/informe.md`. Referencia: en 2019, "entidad → nombre" tenía 8 casos (nombre y sigla).

# COMMAND ----------

display(plata.revisar_dependencias(bronce))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Construir y escribir las 10 tablas
# MAGIC
# MAGIC Cada tabla se escribe con `overwrite` (se reconstruye completa: es pequeña y así el
# MAGIC resultado no depende de ejecuciones anteriores) y `overwriteSchema` (si cambiamos una
# MAGIC columna, la tabla se actualiza). Se escriben en orden: primero las tablas padre.
# MAGIC
# MAGIC **Nota sobre serverless:** no usamos `cache()`/`persist()` (no están disponibles). Cada
# MAGIC tabla recalcula desde bronce; con ~500 mil filas es rápido. Si fuera lento, la solución
# MAGIC es materializar un paso intermedio como tabla Delta, no cachear.

# COMMAND ----------

for nombre_corto, df in plata.construir_plata(bronce).items():
    exigir_sin_personales(df.columns)  # ninguna tabla de plata puede tener datos personales
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(config.tabla(nombre_corto))
    )
    print("escrita:", config.tabla(nombre_corto))

# Desde aquí se trabaja con las tablas YA escritas (no con los planes en memoria).
t = {nombre: spark.table(config.tabla(nombre)) for nombre in plata.CONSTRUCTORES}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Llaves primarias: únicas y sin nulos
# MAGIC Una llave primaria identifica cada fila: no puede repetirse ni estar vacía.

# COMMAND ----------

unicidad = []
for tabla_nombre, llave in plata.LLAVES_PRIMARIAS.items():
    nulos = F.lit(False)
    for c in llave:
        nulos = nulos | F.col(c).isNull()
    fila = (
        t[tabla_nombre]
        .agg(
            F.count("*").alias("filas"),
            F.countDistinct(*llave).alias("llaves"),
            F.sum(nulos.cast("long")).alias("nulos"),
        )
        .first()
    )
    unicidad.append(
        (tabla_nombre, "+".join(llave), fila["filas"], fila["llaves"], fila["nulos"] or 0)
    )
display(
    spark.createDataFrame(
        unicidad, "tabla string, llave string, filas long, llaves_distintas long, llaves_nulas long"
    )
)
assert all(f == k and n == 0 for _, _, f, k, n in unicidad), "Llave primaria duplicada o nula"
print("Llaves primarias: OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Integridad referencial
# MAGIC Cada llave foránea debe existir en su tabla padre (incluida la llave compuesta
# MAGIC departamento + municipio). `left_anti` devuelve las filas del hijo **sin** pareja en el
# MAGIC padre: el conteo debe ser **0**. Si no lo es, el notebook se detiene (mejor fallar aquí
# MAGIC que entrenar con datos rotos).

# COMMAND ----------

resultados = []
for hijo, padre, llave in plata.RELACIONES:
    huerfanas = plata.filas_huerfanas(t[hijo], t[padre], llave).count()
    texto_llave = llave if isinstance(llave, str) else "+".join(llave)
    resultados.append((hijo, padre, texto_llave, huerfanas))

display(
    spark.createDataFrame(resultados, "hijo string, padre string, llave string, huerfanas long")
)
rotas = [r for r in resultados if r[3] != 0]
assert not rotas, f"Integridad referencial rota: {rotas}"
print(f"Integridad referencial: OK (0 huérfanas en las {len(resultados)} relaciones)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Casos que resolvió la normalización
# MAGIC - Entidades que tenían más de un nombre (en 2019 se observaron 8: nombre completo y sigla).
# MAGIC - Procesos con más de una modalidad en la fuente (si hay, se usó la más reciente).
# MAGIC - Municipios con el mismo nombre en varios departamentos (justifican la llave compuesta).

# COMMAND ----------

display(
    t["dim_entidad"].filter("n_nombres > 1").select("codigo_entidad", "nombre_entidad", "n_nombres")
)
display(t["dim_proceso"].filter("n_modalidades > 1").limit(20))
display(
    t["dim_municipio"]
    .groupBy("municipio")
    .agg(F.count("*").alias("departamentos"))
    .filter("departamentos > 1")
    .orderBy(F.desc("departamentos"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Vistazo a las limpiezas
# MAGIC Estado unificado (ya sin mayúsculas inconsistentes) y duración separada en número y unidad.

# COMMAND ----------

display(t["contrato_ejecucion"].groupBy("estado_contrato").count().orderBy(F.desc("count")))
display(
    t["contrato"]
    .groupBy("duracion_unidad")
    .agg(F.count("*").alias("contratos"), F.avg("duracion_numero").alias("numero_promedio"))
)
