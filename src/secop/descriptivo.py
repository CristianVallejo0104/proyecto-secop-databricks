"""Agregaciones descriptivas (se calculan en Spark y se muestran con entorno.mostrar)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from secop.config import COLUMNA_ETIQUETA


def tasa_por_grupo(
    df: DataFrame,
    grupos: list[str],
    min_contratos: int = 0,
    col_etiqueta: str = COLUMNA_ETIQUETA,
) -> DataFrame:
    """Número de contratos, prorrogados y tasa de prórroga (%) por grupo.

    ``min_contratos`` evita conclusiones con grupos diminutos: una entidad con
    2 contratos y 1 prórroga tendría una tasa de 50 % que no significa nada.
    El resultado es pequeño (una fila por grupo), así que mostrarlo es barato.
    """
    return (
        df.groupBy(*grupos)
        .agg(
            F.count("*").alias("contratos"),
            F.sum(col_etiqueta).alias("prorrogados"),
            F.round(F.avg(col_etiqueta) * 100, 2).alias("tasa_prorroga_pct"),
        )
        .filter(F.col("contratos") >= min_contratos)
        .orderBy(F.desc("tasa_prorroga_pct"), F.desc("contratos"))
    )
