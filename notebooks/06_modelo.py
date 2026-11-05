# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Modelo: ¿se prorrogará el contrato?
# MAGIC
# MAGIC **Qué hace:** entrena y evalúa clasificadores con variables conocidas al firmar, y
# MAGIC registra todo en **MLflow** (parámetros, métricas, versión del código).
# MAGIC
# MAGIC **Diseño (cada punto se justifica en `docs/decisiones.md`):**
# MAGIC 1. **Partición temporal** por fecha de firma: entrenar con el pasado, evaluar con el futuro.
# MAGIC 2. **Pesos de clase:** con ~11 % de positivos, cada positivo pesa más en el entrenamiento.
# MAGIC 3. **Línea base:** regresión logística. **Modelo de árboles:** GBT (árboles potenciados).
# MAGIC 4. **Métricas:** AUC-ROC, AUC-PR y F1, no solo exactitud. Un modelo que diga siempre
# MAGIC    "no hay prórroga" tendría ~89 % de exactitud y no serviría para nada.
# MAGIC 5. **Umbral** elegido en validación (máximo F1) y aplicado una sola vez en prueba.
# MAGIC 6. **Prueba de fuga:** se comparan tres variantes: sin plazo, con duración declarada y con
# MAGIC    plazo (fin − inicio). Si el AUC salta de forma sospechosa al añadir el plazo, la fecha de
# MAGIC    fin probablemente ya incluye la prórroga.
# MAGIC 7. **Plan B:** si Spark MLlib falla en serverless, se usa scikit-learn sobre una muestra
# MAGIC    (la preparación de datos sigue en Spark).
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

import secop
from secop import config, modelos
from secop import modelado as M

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros
# MAGIC - `corte_validacion` / `corte_prueba`: fechas que separan entrenamiento, validación y prueba.
# MAGIC   Por defecto: entrenamiento = 2019 a marzo de 2020, validación = abril a junio de 2020,
# MAGIC   prueba = julio a diciembre de 2020. Ajustar si se cargan más años (nunca 2025-2026).
# MAGIC - `motor`: `auto` intenta MLlib y, si falla, usa scikit-learn; o forzar uno de los dos.
# MAGIC - `max_filas_muestra`: tope de filas por partición para el plan B.
# MAGIC - `commit`: hash de git del código (cópialo del menú Git del Git folder). Se registra en MLflow.

# COMMAND ----------

dbutils.widgets.text("corte_validacion", config.CORTE_VALIDACION, "Corte validación")
dbutils.widgets.text("corte_prueba", config.CORTE_PRUEBA, "Corte prueba")
dbutils.widgets.dropdown("motor", "auto", ["auto", "mllib", "sklearn"], "Motor")
dbutils.widgets.text("variantes", ",".join(config.VARIANTES), "Variantes")
dbutils.widgets.text("iteraciones_gbt", "40", "Iteraciones GBT")
dbutils.widgets.text("max_filas_muestra", "200000", "Máx. filas plan B")
dbutils.widgets.text("commit", "", "Commit de git")

CORTE_VAL = dbutils.widgets.get("corte_validacion")
CORTE_PRU = dbutils.widgets.get("corte_prueba")
MOTOR = dbutils.widgets.get("motor")
VARIANTES = [v.strip() for v in dbutils.widgets.get("variantes").split(",") if v.strip()]
ITER_GBT = int(dbutils.widgets.get("iteraciones_gbt"))
MAX_FILAS = int(dbutils.widgets.get("max_filas_muestra"))
COMMIT = dbutils.widgets.get("commit").strip() or "no_indicado"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Datos y partición temporal
# MAGIC Se quitan los años con etiqueta incompleta y se parte por fecha de firma.
# MAGIC Una sola agregación cuenta filas y positivos en las tres particiones.

# COMMAND ----------

gold = M.solo_etiqueta_completa(spark.table(config.TABLA_ORO))
entrenamiento, validacion, prueba = M.particion_temporal(gold, CORTE_VAL, CORTE_PRU)


def resumen(df, nombre):
    fila = df.agg(F.count("*").alias("n"), F.sum("etiqueta").alias("pos")).first()
    n, pos = int(fila["n"]), int(fila["pos"] or 0)
    return (nombre, n, pos, round(100 * pos / n, 2) if n else None)


tamanos = [
    resumen(entrenamiento, "entrenamiento"),
    resumen(validacion, "validacion"),
    resumen(prueba, "prueba"),
]
display(
    spark.createDataFrame(tamanos, "particion string, filas long, positivos long, tasa_pct double")
)
assert all(t[1] > 0 for t in tamanos), "Alguna partición quedó vacía: revisa los cortes"

# Pesos calculados SOLO con entrenamiento (validación y prueba no se tocan).
PESOS = M.calcular_pesos(entrenamiento)
entrenamiento = M.agregar_peso(entrenamiento, PESOS)
print("Pesos de clase:", PESOS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. MLflow: experimento y etiquetas de reproducibilidad
# MAGIC Cada combinación (variante × algoritmo) es un *run* hijo dentro de un *run* padre.
# MAGIC Se registran: versión del paquete `secop`, commit de git, versión de Spark, semilla y cortes.

# COMMAND ----------

import mlflow

usuario = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{usuario}/secop_prorrogas")

ETIQUETAS = {
    "version_codigo": secop.__version__,
    "commit_git": COMMIT,
    "spark_version": spark.version,
    "tabla_oro": config.TABLA_ORO,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Plan A: Spark MLlib
# MAGIC El pipeline (definido y probado en `src/secop/modelos.py`) se entrena con `fit` sobre
# MAGIC entrenamiento y se aplica con `transform` a validación y prueba. Las métricas se calculan
# MAGIC con agregaciones de Spark: las predicciones nunca salen del clúster.

# COMMAND ----------


def evaluar_mllib(variante: str, algoritmo: str) -> dict:
    cats, nums = M.variables_de_variante(variante)
    pipeline = modelos.pipeline_mllib(cats, nums, algoritmo, iteraciones_arboles=ITER_GBT)
    modelo = pipeline.fit(entrenamiento)
    pred_val = modelos.probabilidad_positiva(modelo.transform(validacion))
    pred_pru = modelos.probabilidad_positiva(modelo.transform(prueba))
    # Umbral: el de mayor F1 en VALIDACIÓN.
    umbral = M.elegir_umbral(M.metricas_por_umbral(pred_val, "prob", M.UMBRALES_POR_DEFECTO))[
        "umbral"
    ]
    (m,) = M.metricas_por_umbral(pred_pru, "prob", [umbral])
    n_pru = tamanos[2][1]
    exactitud = (n_pru - m["fp"] - m["fn"]) / n_pru
    return {
        "modelo": modelo,
        "umbral": umbral,
        "auc_roc_val": modelos.auc_spark(pred_val, "areaUnderROC"),
        "auc_pr_val": modelos.auc_spark(pred_val, "areaUnderPR"),
        "auc_roc_prueba": modelos.auc_spark(pred_pru, "areaUnderROC"),
        "auc_pr_prueba": modelos.auc_spark(pred_pru, "areaUnderPR"),
        "f1_prueba": m["f1"],
        "precision_prueba": m["precision"],
        "sensibilidad_prueba": m["sensibilidad"],
        "exactitud_prueba": exactitud,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Plan B: scikit-learn sobre una muestra
# MAGIC Si MLlib no está disponible en serverless, la preparación sigue en Spark (filtros,
# MAGIC partición, selección de columnas) y solo una **muestra acotada** pasa a pandas.
# MAGIC Este es el único `toPandas()` del proyecto, y se hace sobre como máximo `max_filas_muestra`
# MAGIC filas por partición y unas 30 columnas (decenas de MB), no sobre la tabla completa.
# MAGIC La muestra es aleatoria con semilla fija dentro de cada partición temporal.

# COMMAND ----------


def muestra_pandas(df, columnas):
    n = df.count()
    fraccion = min(1.0, MAX_FILAS / n) if n else 1.0
    return df.select(*columnas).sample(fraction=fraccion, seed=config.SEMILLA).toPandas()


def evaluar_sklearn(variante: str, algoritmo: str) -> dict:
    cats, nums = M.variables_de_variante(variante)
    columnas = cats + nums + ["etiqueta"]
    ent, val, pru = (muestra_pandas(d, columnas) for d in (entrenamiento, validacion, prueba))
    modelo = modelos.pipeline_sklearn(cats, nums, algoritmo)
    modelo.fit(ent[cats + nums], ent["etiqueta"])  # class_weight="balanced" dentro del modelo
    p_val = modelo.predict_proba(val[cats + nums])[:, 1]
    p_pru = modelo.predict_proba(pru[cats + nums])[:, 1]
    umbral = modelos.umbral_optimo_sklearn(val["etiqueta"], p_val, M.UMBRALES_POR_DEFECTO)
    m_val = modelos.metricas_sklearn(val["etiqueta"], p_val, umbral)
    m_pru = modelos.metricas_sklearn(pru["etiqueta"], p_pru, umbral)
    return {
        "modelo": modelo,
        "umbral": umbral,
        "auc_roc_val": m_val["auc_roc"],
        "auc_pr_val": m_val["auc_pr"],
        "auc_roc_prueba": m_pru["auc_roc"],
        "auc_pr_prueba": m_pru["auc_pr"],
        "f1_prueba": m_pru["f1"],
        "precision_prueba": m_pru["precision"],
        "sensibilidad_prueba": m_pru["sensibilidad"],
        "exactitud_prueba": m_pru["exactitud"],
        "filas_muestra_entrenamiento": len(ent),
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Elegir el motor
# MAGIC Con `motor = auto` se hace una prueba pequeña de MLlib. Si falla (por ejemplo, porque
# MAGIC serverless no admite alguna etapa), se cambia a scikit-learn y se registra el motivo.

# COMMAND ----------

motor_usado = MOTOR
motivo_plan_b = ""
if MOTOR == "auto":
    try:
        cats, nums = M.variables_de_variante("sin_plazo")
        modelos.pipeline_mllib(cats, nums, "logistica").fit(entrenamiento.limit(2000))
        motor_usado = "mllib"
    except Exception as error:  # noqa: BLE001 - queremos cualquier fallo de MLlib
        motor_usado = "sklearn"
        motivo_plan_b = f"{type(error).__name__}: {str(error)[:300]}"
if MOTOR != "auto":
    motivo_plan_b = "" if MOTOR == "mllib" else "elegido en el widget"
print("Motor:", motor_usado, "|", motivo_plan_b or "MLlib disponible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Entrenar todas las combinaciones y registrar en MLflow

# COMMAND ----------

evaluar = evaluar_mllib if motor_usado == "mllib" else evaluar_sklearn
resultados = []

with mlflow.start_run(run_name=f"comparacion_{motor_usado}") as padre:
    mlflow.set_tags({**ETIQUETAS, "motor": motor_usado, "motivo_plan_b": motivo_plan_b})
    mlflow.log_params(
        {
            "corte_validacion": CORTE_VAL,
            "corte_prueba": CORTE_PRU,
            "semilla": config.SEMILLA,
            "peso_clase_0": round(PESOS[0], 4),
            "peso_clase_1": round(PESOS[1], 4),
            "filas_entrenamiento": tamanos[0][1],
            "filas_prueba": tamanos[2][1],
        }
    )
    # Referencia trivial: un modelo sin información tiene AUC-ROC 0,5 y AUC-PR = tasa de positivos.
    mlflow.log_metric("auc_pr_azar_prueba", tamanos[2][2] / tamanos[2][1])

    for variante in VARIANTES:
        for algoritmo in modelos.ALGORITMOS:
            with mlflow.start_run(run_name=f"{variante}_{algoritmo}", nested=True):
                mlflow.set_tags({**ETIQUETAS, "motor": motor_usado})
                cats, nums = M.variables_de_variante(variante)
                mlflow.log_params(
                    {
                        "variante": variante,
                        "algoritmo": algoritmo,
                        "n_categoricas": len(cats),
                        "numericas": ",".join(nums),
                        "iteraciones_gbt": ITER_GBT if algoritmo == "arboles" else 0,
                    }
                )
                r = evaluar(variante, algoritmo)
                metricas = {k: float(v) for k, v in r.items() if k != "modelo"}
                mlflow.log_metrics(metricas)
                # Guardar el modelo es opcional: en serverless puede requerir un Volume temporal.
                try:
                    if motor_usado == "mllib":
                        mlflow.spark.log_model(
                            r["modelo"],
                            "modelo",
                            dfs_tmpdir=f"/Volumes/{config.CATALOGO}/{config.ESQUEMA}/{config.VOLUMEN}/tmp_mlflow",
                        )
                    else:
                        mlflow.sklearn.log_model(r["modelo"], "modelo")
                except Exception as error:  # noqa: BLE001
                    mlflow.set_tag("modelo_no_guardado", str(error)[:250])
                resultados.append(
                    (
                        variante,
                        algoritmo,
                        *[
                            round(metricas[k], 4)
                            for k in (
                                "auc_roc_prueba",
                                "auc_pr_prueba",
                                "f1_prueba",
                                "precision_prueba",
                                "sensibilidad_prueba",
                                "exactitud_prueba",
                                "umbral",
                            )
                        ],
                    )
                )
                print("listo:", variante, algoritmo, round(metricas["auc_roc_prueba"], 4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Comparación de resultados (en prueba)

# COMMAND ----------

tabla_resultados = spark.createDataFrame(
    resultados,
    "variante string, algoritmo string, auc_roc double, auc_pr double, f1 double, "
    "precision double, sensibilidad double, exactitud double, umbral double",
)
display(tabla_resultados.orderBy("algoritmo", "variante"))
print("AUC-PR de referencia (azar) en prueba:", round(tamanos[2][2] / tamanos[2][1], 4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. ¿Hay fuga en el plazo?
# MAGIC Regla práctica (no es una prueba formal): si añadir el plazo sube el AUC-ROC en más de
# MAGIC **0,05** respecto a `sin_plazo`, lo tratamos como **sospechoso**: una sola variable no
# MAGIC debería mejorar tanto un modelo que ya tiene más de 20, salvo que contenga la respuesta.
# MAGIC En ese caso el modelo final es `sin_plazo`. Interpretación: PENDIENTE.

# COMMAND ----------

auc = {(v, a): roc for v, a, roc, *_ in resultados}
for algoritmo in modelos.ALGORITMOS:
    base = auc.get(("sin_plazo", algoritmo))
    for variante in [v for v in VARIANTES if v != "sin_plazo"]:
        if base is None or (variante, algoritmo) not in auc:
            continue
        salto = auc[(variante, algoritmo)] - base
        estado = "SOSPECHOSO (posible fuga)" if salto > 0.05 else "sin salto llamativo"
        print(f"{algoritmo:9s} {variante:13s} ΔAUC = {salto:+.4f} -> {estado}")
