"""Utilidades de modelado independientes del algoritmo (Spark MLlib o scikit-learn).

Todas las métricas se calculan con agregaciones de Spark: no hace falta traer
las predicciones al driver con toPandas().
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from secop.config import (
    ANIOS_ETIQUETA_INCOMPLETA,
    COLUMNA_ETIQUETA,
    COLUMNA_PESO,
    VARIABLES_CATEGORICAS,
    VARIABLES_NUMERICAS,
    VARIANTES,
)


def variables_de_variante(variante: str) -> tuple[list[str], list[str]]:
    """(categóricas, numéricas) que usa una variante definida en config.VARIANTES."""
    if variante not in VARIANTES:
        raise ValueError(f"Variante desconocida: {variante}. Opciones: {list(VARIANTES)}")
    return list(VARIABLES_CATEGORICAS), list(VARIABLES_NUMERICAS) + VARIANTES[variante]


def solo_etiqueta_completa(df: DataFrame, col_anio: str = "anio_firma") -> DataFrame:
    """Quita los años cuya etiqueta aún puede cambiar (2025-2026)."""
    return df.filter(~F.col(col_anio).isin(*ANIOS_ETIQUETA_INCOMPLETA))


def particion_temporal(
    df: DataFrame, corte_validacion: str, corte_prueba: str, col_fecha: str = "fecha_firma"
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Parte los datos por fecha de firma: pasado para entrenar, futuro para evaluar.

    entrenamiento: fecha < corte_validacion
    validación:    corte_validacion <= fecha < corte_prueba  (elegir el umbral)
    prueba:        fecha >= corte_prueba                       (métrica final)

    ¿Por qué no al azar? El modelo se usaría con contratos futuros; mezclar
    meses permitiría "ver el futuro" y daría métricas optimistas.
    """
    if corte_validacion >= corte_prueba:
        raise ValueError("corte_validacion debe ser anterior a corte_prueba")
    fecha = F.col(col_fecha)
    entrenamiento = df.filter(fecha < F.lit(corte_validacion).cast("date"))
    validacion = df.filter(
        (fecha >= F.lit(corte_validacion).cast("date")) & (fecha < F.lit(corte_prueba).cast("date"))
    )
    prueba = df.filter(fecha >= F.lit(corte_prueba).cast("date"))
    return entrenamiento, validacion, prueba


def calcular_pesos(df: DataFrame, col_etiqueta: str = COLUMNA_ETIQUETA) -> dict[int, float]:
    """Pesos de clase "balanceados": peso_k = n / (2 * n_k).

    Con ~11 % de positivos, cada positivo pesa ≈ 4,5 y cada negativo ≈ 0,56,
    de modo que ambas clases aportan lo mismo a la función de pérdida. Sin
    esto, el modelo puede predecir siempre "no hay prórroga" y acertar 89 %.
    """
    fila = df.agg(F.count("*").alias("n"), F.sum(F.col(col_etiqueta)).alias("n_pos")).first()
    n, n_pos = int(fila["n"]), int(fila["n_pos"] or 0)
    n_neg = n - n_pos
    if n_pos == 0 or n_neg == 0:
        raise ValueError("Se necesitan ambas clases para calcular pesos")
    return {0: n / (2.0 * n_neg), 1: n / (2.0 * n_pos)}


def agregar_peso(
    df: DataFrame, pesos: dict[int, float], col_etiqueta: str = COLUMNA_ETIQUETA
) -> DataFrame:
    """Agrega la columna de peso según la clase de cada fila."""
    return df.withColumn(
        COLUMNA_PESO,
        F.when(F.col(col_etiqueta) == 1, F.lit(pesos[1])).otherwise(F.lit(pesos[0])),
    )


def metricas_por_umbral(
    df: DataFrame, col_prob: str, umbrales: list[float], col_etiqueta: str = COLUMNA_ETIQUETA
) -> list[dict]:
    """Matriz de confusión, precisión, sensibilidad y F1 para varios umbrales.

    Una sola pasada por los datos: para cada umbral se cuentan VP, FP y FN con
    sumas condicionales dentro del mismo ``agg``.
    """
    y, p = F.col(col_etiqueta), F.col(col_prob)
    expresiones = []
    for i, u in enumerate(umbrales):
        expresiones += [
            F.sum(((p >= u) & (y == 1)).cast("long")).alias(f"vp_{i}"),
            F.sum(((p >= u) & (y == 0)).cast("long")).alias(f"fp_{i}"),
            F.sum(((p < u) & (y == 1)).cast("long")).alias(f"fn_{i}"),
        ]
    fila = df.agg(*expresiones).first()
    resultados = []
    for i, u in enumerate(umbrales):
        vp, fp, fn = (int(fila[f"{k}_{i}"] or 0) for k in ("vp", "fp", "fn"))
        precision = vp / (vp + fp) if vp + fp else 0.0
        sensibilidad = vp / (vp + fn) if vp + fn else 0.0
        f1 = (
            2 * precision * sensibilidad / (precision + sensibilidad)
            if precision + sensibilidad
            else 0.0
        )
        resultados.append(
            {
                "umbral": u,
                "vp": vp,
                "fp": fp,
                "fn": fn,
                "precision": precision,
                "sensibilidad": sensibilidad,
                "f1": f1,
            }
        )
    return resultados


def elegir_umbral(metricas: list[dict]) -> dict:
    """El umbral con mayor F1 (se elige en VALIDACIÓN, nunca en prueba)."""
    return max(metricas, key=lambda m: (m["f1"], -m["umbral"]))


UMBRALES_POR_DEFECTO = [round(0.05 * i, 2) for i in range(1, 20)]  # 0.05 ... 0.95
