"""Capa bronce: copia fiel de la fuente, todo como texto, sin datos personales.

Bronce no corrige nada (eso es trabajo de plata). Solo:
1. Verifica que lleguen las columnas esperadas.
2. Elimina las columnas con datos personales (nunca se escriben en una tabla).
3. Deja todo como string y agrega de qué año de carga y archivo viene cada fila.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from secop.esquema import COLUMNAS_BRONCE
from secop.validacion import exigir_columnas


def preparar_bronce(df_crudo: DataFrame, anio_carga: int) -> DataFrame:
    """Convierte el CSV leído de un año en filas listas para la tabla bronce.

    Parámetros
    ----------
    df_crudo: DataFrame leído de los CSV de ``anio=AAAA`` (columnas en texto).
        Si trae la columna ``archivo_origen`` (nombre del archivo), se conserva.
    anio_carga: año de la carpeta de la que se leyó. Es la columna de partición.
    """
    # 1. Si falta una columna, es mejor fallar aquí que en plata con un error confuso.
    exigir_columnas(df_crudo.columns)
    # 2 y 3. Seleccionar SOLO las columnas permitidas (las personales quedan fuera)
    #    y forzar tipo string para que todos los años tengan el mismo esquema.
    columnas = [F.col(c).cast("string").alias(c) for c in COLUMNAS_BRONCE]
    if "archivo_origen" in df_crudo.columns:
        columnas.append(F.col("archivo_origen").cast("string"))
    else:
        columnas.append(F.lit(None).cast("string").alias("archivo_origen"))
    # anio_carga como entero: es la partición física de la tabla Delta.
    columnas.append(F.lit(int(anio_carga)).cast("int").alias("anio_carga"))
    return df_crudo.select(*columnas)
