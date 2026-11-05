"""Capa oro: una fila por contrato, lista para el modelo y los descriptivos.

Contiene la etiqueta (¿hubo prórroga?) y SOLO variables conocidas al firmar,
más las variables de plazo, que se marcan aparte porque podrían tener fuga.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from secop.config import (
    ANIOS_ETIQUETA_INCOMPLETA,
    COLUMNA_ETIQUETA,
    TOPE_VALOR_CONTRATO,
    VARIABLES_CATEGORICAS,
    VARIABLES_NUMERICAS,
    VARIABLES_PLAZO,
)
from secop.limpieza import limpiar_categoria


def construir_etiqueta(dias_adicionados: str | F.Column) -> F.Column:
    """1 si al contrato se le añadieron días (> 0), 0 si no, NULL si se desconoce.

    Un NULL en dias_adicionados no se interpreta como "0 días": no sabemos qué
    pasó, así que esa fila no tiene etiqueta y se excluye de oro.
    """
    dias = F.col(dias_adicionados) if isinstance(dias_adicionados, str) else dias_adicionados
    return F.when(dias > 0, 1).when(dias.isNotNull(), 0).cast("int")


def construir_oro(
    contrato: DataFrame,
    dim_entidad: DataFrame,
    dim_proveedor: DataFrame,
    dim_categoria: DataFrame,
    ejecucion: DataFrame,
    tope_valor: float = TOPE_VALOR_CONTRATO,
) -> DataFrame:
    """Une las tablas de plata y calcula las variables del modelo.

    Filtros (cada uno explicado en docs/decisiones.md):
    - fecha de firma conocida (sin ella no hay partición temporal);
    - valor del contrato > 0 (los <= 0 son errores de registro);
    - etiqueta conocida (dias_adicionados no nulo).
    """
    # Las dimensiones de entidad y categoría son pequeñas: broadcast envía una
    # copia a cada ejecutor y evita mover (shuffle) la tabla grande de contratos.
    entidad = F.broadcast(
        dim_entidad.select(
            "codigo_entidad",
            "orden_entidad",
            "sector_entidad",
            "rama_entidad",
            "entidad_centralizada",
            "departamento_entidad",
        )
    )
    categoria = F.broadcast(
        dim_categoria.select("codigo_categoria", "familia_unspsc", "segmento_unspsc")
    )
    proveedor = dim_proveedor.select("codigo_proveedor", "tipo_doc_proveedor")
    # De ejecución SOLO se toma dias_adicionados, y solo para la etiqueta.
    etiqueta = ejecucion.select(
        "id_contrato", construir_etiqueta("dias_adicionados").alias(COLUMNA_ETIQUETA)
    )

    unido = (
        contrato.join(entidad, "codigo_entidad", "left")
        .join(categoria, "codigo_categoria", "left")
        .join(proveedor, "codigo_proveedor", "left")
        .join(etiqueta, "id_contrato", "left")
    )

    variables = {
        # Monetarias: recorte en el tope y logaritmo
        "log_valor_contrato": F.log1p(F.least("valor_contrato", F.lit(float(tope_valor)))),
        # Qué fracción del valor se paga por adelantado (entre 0 y 1)
        "proporcion_pago_adelantado": F.least(
            F.coalesce("valor_pago_adelantado", F.lit(0.0)) / F.col("valor_contrato"),
            F.lit(1.0),
        ),
        # Calendario de la firma (estacionalidad: fin de año, ley de garantías...)
        "anio_firma": F.year("fecha_firma"),
        "mes_firma": F.month("fecha_firma"),
        "dia_semana_firma": F.dayofweek("fecha_firma"),
        # Diferencia firma-inicio. Negativa si el contrato "empezó" antes de
        # firmarse (~48 % en 2019, típico de registros hechos después).
        "dias_firma_a_inicio": F.datediff("fecha_inicio", "fecha_firma"),
        "inicio_antes_de_firma": (F.col("fecha_inicio") < F.col("fecha_firma")).cast("int"),
        # PLAZO (sospechoso de fuga, ver config.VARIANTES)
        "plazo_dias": F.datediff("fecha_fin", "fecha_inicio"),
    }
    # Las categorías que quedaron NULL por un join sin pareja pasan a "No Definido".
    categoricas = {c: limpiar_categoria(c) for c in VARIABLES_CATEGORICAS}

    return (
        unido.withColumns(variables)
        .withColumns(categoricas)
        .withColumn("etiqueta_completa", ~F.col("anio_firma").isin(*ANIOS_ETIQUETA_INCOMPLETA))
        .filter(F.col("fecha_firma").isNotNull())
        .filter(F.col("valor_contrato").isNotNull())
        .filter(F.col(COLUMNA_ETIQUETA).isNotNull())
        .select(
            "id_contrato",
            "fecha_firma",
            "anio_firma",
            *VARIABLES_CATEGORICAS,
            *VARIABLES_NUMERICAS,
            *VARIABLES_PLAZO,
            COLUMNA_ETIQUETA,
            "etiqueta_completa",
        )
    )
