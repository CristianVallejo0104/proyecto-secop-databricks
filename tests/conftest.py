"""Configuración compartida de pytest: sesión de Spark local y datos sintéticos.

Los tests corren con Spark en modo local (un solo proceso en este computador),
con el modo ANSI activado, igual que la sesión de los notebooks, donde un cast
inválido detiene el trabajo en lugar de devolver NULL.
"""

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark no está instalado")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

from secop.bronce import preparar_bronce  # noqa: E402
from secop.esquema import COLUMNAS_CRUDAS  # noqa: E402
from secop.sinteticos import generar_filas  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    """Una sola sesión de Spark para todos los tests (crearla tarda segundos)."""
    sesion = (
        SparkSession.builder.master("local[1]")
        .appName("tests-secop")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.log.level", "ERROR")
        .getOrCreate()
    )
    yield sesion
    sesion.stop()


@pytest.fixture(scope="session")
def df_crudo(spark):
    """DataFrame crudo sintético con las 95 columnas como texto."""
    esquema = StructType([StructField(c, StringType()) for c in COLUMNAS_CRUDAS])
    filas = [tuple(f[c] or None for c in COLUMNAS_CRUDAS) for f in generar_filas(80)]
    return spark.createDataFrame(filas, esquema)


@pytest.fixture(scope="session")
def df_bronce(df_crudo):
    """El mismo DataFrame después de pasar por la capa bronce."""
    return preparar_bronce(df_crudo, 2019)
