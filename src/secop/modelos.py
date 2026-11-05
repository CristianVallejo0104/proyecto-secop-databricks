"""Construcción de los modelos: plan A con Spark MLlib y plan B con scikit-learn.

Dos algoritmos en cada plan:
- ``logistica``: regresión logística, la LÍNEA BASE (simple e interpretable).
- ``arboles``: árboles potenciados (GBT en Spark, HistGradientBoosting en
  scikit-learn), que capturan interacciones y relaciones no lineales.

Ambos usan pesos de clase por el desbalance (~11 % de positivos).

Los imports de Spark ML y scikit-learn van DENTRO de las funciones: así este
módulo se puede importar aunque una de las dos bibliotecas no esté instalada.
"""

from secop.config import COLUMNA_ETIQUETA, COLUMNA_PESO, SEMILLA

ALGORITMOS = ("logistica", "arboles")


# ---------------------------------------------------------------------------
# Plan A: Spark MLlib (los datos nunca salen de Spark)
# ---------------------------------------------------------------------------
def pipeline_mllib(
    categoricas: list[str],
    numericas: list[str],
    algoritmo: str,
    iteraciones_arboles: int = 40,
):
    """Pipeline de Spark ML: preparar variables + clasificador.

    Etapas:
    1. StringIndexer: texto -> índice numérico (handleInvalid="keep" reserva un
       índice para categorías que aparezcan en prueba y no en entrenamiento).
    2. OneHotEncoder: índice -> vector binario (una columna por categoría).
    3. Imputer: rellena vacíos numéricos con la MEDIANA de entrenamiento.
    4. VectorAssembler: junta todo en un solo vector "features".
    5. Clasificador con weightCol="peso".
    Todas las etapas "aprenden" (fit) solo con entrenamiento: las medianas y
    categorías de validación/prueba nunca influyen en el modelo.
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import GBTClassifier, LogisticRegression
    from pyspark.ml.feature import (
        Imputer,
        OneHotEncoder,
        StandardScaler,
        StringIndexer,
        VectorAssembler,
    )

    if algoritmo not in ALGORITMOS:
        raise ValueError(f"Algoritmo desconocido: {algoritmo}")
    indices = [f"{c}__idx" for c in categoricas]
    binarias = [f"{c}__ohe" for c in categoricas]
    imputadas = [f"{c}__imp" for c in numericas]
    etapas = [
        StringIndexer(inputCols=categoricas, outputCols=indices, handleInvalid="keep"),
        OneHotEncoder(inputCols=indices, outputCols=binarias, handleInvalid="keep"),
        Imputer(inputCols=numericas, outputCols=imputadas, strategy="median"),
        VectorAssembler(inputCols=binarias + imputadas, outputCol="features"),
    ]
    if algoritmo == "logistica":
        # Escalar ayuda a que la regularización trate igual a todas las variables.
        # withMean=False mantiene el vector disperso (no resta la media).
        etapas.append(StandardScaler(inputCol="features", outputCol="features_esc"))
        etapas.append(
            LogisticRegression(
                featuresCol="features_esc",
                labelCol=COLUMNA_ETIQUETA,
                weightCol=COLUMNA_PESO,
                maxIter=100,
                regParam=0.01,
                elasticNetParam=0.0,
            )
        )
    else:
        etapas.append(
            GBTClassifier(
                featuresCol="features",
                labelCol=COLUMNA_ETIQUETA,
                weightCol=COLUMNA_PESO,
                maxIter=iteraciones_arboles,
                maxDepth=5,
                stepSize=0.1,
                seed=SEMILLA,
            )
        )
    return Pipeline(stages=etapas)


def probabilidad_positiva(predicciones, col_salida: str = "prob"):
    """Extrae P(etiqueta = 1) del vector "probability" de Spark ML como columna double."""
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F

    return predicciones.withColumn(col_salida, vector_to_array(F.col("probability"))[1])


def auc_spark(predicciones, metrica: str) -> float:
    """AUC-ROC ("areaUnderROC") o AUC-PR ("areaUnderPR") calculada por Spark."""
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    evaluador = BinaryClassificationEvaluator(
        rawPredictionCol="probability", labelCol=COLUMNA_ETIQUETA, metricName=metrica
    )
    return float(evaluador.evaluate(predicciones))


# ---------------------------------------------------------------------------
# Plan B: scikit-learn sobre una MUESTRA (si MLlib falla en serverless)
# ---------------------------------------------------------------------------
def pipeline_sklearn(categoricas: list[str], numericas: list[str], algoritmo: str):
    """Pipeline equivalente en scikit-learn.

    - logistica: one-hot (categorías con < 50 casos se agrupan como "infrecuentes"),
      mediana para vacíos, escalado y LogisticRegression(class_weight="balanced").
    - arboles: códigos ordinales (máx. 250 categorías por variable, el límite de
      HistGradientBoosting) y HistGradientBoostingClassifier(class_weight="balanced"),
      que maneja vacíos numéricos sin imputar.
    """
    import numpy as np
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline, make_pipeline
    from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler

    if algoritmo == "logistica":
        preparar = ColumnTransformer(
            [
                (
                    "cat",
                    OneHotEncoder(handle_unknown="infrequent_if_exist", min_frequency=50),
                    categoricas,
                ),
                (
                    "num",
                    make_pipeline(SimpleImputer(strategy="median"), StandardScaler()),
                    numericas,
                ),
            ]
        )
        modelo = LogisticRegression(class_weight="balanced", max_iter=1000, C=1.0)
    elif algoritmo == "arboles":
        preparar = ColumnTransformer(
            [
                (
                    "cat",
                    OrdinalEncoder(
                        handle_unknown="use_encoded_value",
                        unknown_value=np.nan,
                        max_categories=250,
                    ),
                    categoricas,
                ),
                ("num", "passthrough", numericas),
            ]
        )
        # Las primeras len(categoricas) columnas de salida son categóricas.
        mascara = [True] * len(categoricas) + [False] * len(numericas)
        modelo = HistGradientBoostingClassifier(
            categorical_features=mascara,
            class_weight="balanced",
            max_iter=300,
            learning_rate=0.1,
            early_stopping=True,
            random_state=SEMILLA,
        )
    else:
        raise ValueError(f"Algoritmo desconocido: {algoritmo}")
    return Pipeline([("preparar", preparar), ("modelo", modelo)])


def metricas_sklearn(y, prob, umbral: float) -> dict:
    """AUC-ROC, AUC-PR, F1, precisión, sensibilidad y exactitud con un umbral dado."""
    from sklearn.metrics import (
        accuracy_score,
        average_precision_score,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    pred = (prob >= umbral).astype(int)
    return {
        "auc_roc": float(roc_auc_score(y, prob)),
        "auc_pr": float(average_precision_score(y, prob)),
        "f1": float(f1_score(y, pred, zero_division=0)),
        "precision": float(precision_score(y, pred, zero_division=0)),
        "sensibilidad": float(recall_score(y, pred, zero_division=0)),
        "exactitud": float(accuracy_score(y, pred)),
    }


def umbral_optimo_sklearn(y, prob, umbrales: list[float]) -> float:
    """Umbral con mayor F1 en validación (misma regla que modelado.elegir_umbral)."""
    from sklearn.metrics import f1_score

    puntajes = [(f1_score(y, (prob >= u).astype(int), zero_division=0), -u) for u in umbrales]
    mejor = max(range(len(umbrales)), key=lambda i: puntajes[i])
    return umbrales[mejor]
