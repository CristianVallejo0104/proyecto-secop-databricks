"""Pruebas de humo de los pipelines: que entrenen y predigan con datos sintéticos.

No evalúan calidad (los datos son aleatorios); comprueban que el código corre
de principio a fin, para no descubrir errores de programación en Databricks.
"""

import pytest

from secop import modelado as M
from secop import modelos, plata
from secop.config import COLUMNA_ETIQUETA
from secop.oro import construir_oro_desde_plata
from secop.sinteticos import generar_filas


@pytest.fixture(scope="module")
def oro_grande(spark):
    """Oro sintético con 400 filas (más que el resto de tests, para entrenar)."""
    from pyspark.sql.types import StringType, StructField, StructType

    from secop.bronce import preparar_bronce
    from secop.esquema import COLUMNAS_CRUDAS

    esquema = StructType([StructField(c, StringType()) for c in COLUMNAS_CRUDAS])
    filas = [tuple(f[c] or None for c in COLUMNAS_CRUDAS) for f in generar_filas(400, 3)]
    bronce = preparar_bronce(spark.createDataFrame(filas, esquema), 2019)
    oro = construir_oro_desde_plata(plata.construir_plata(bronce))
    return M.solo_etiqueta_completa(oro)


@pytest.mark.parametrize("algoritmo", modelos.ALGORITMOS)
def test_pipeline_mllib_entrena_y_predice(oro_grande, algoritmo):
    cats, nums = M.variables_de_variante("con_plazo")
    datos = M.agregar_peso(oro_grande, M.calcular_pesos(oro_grande))
    pipeline = modelos.pipeline_mllib(cats, nums, algoritmo, iteraciones_arboles=3)
    predicciones = modelos.probabilidad_positiva(pipeline.fit(datos).transform(datos))
    auc = modelos.auc_spark(predicciones, "areaUnderROC")
    assert 0.0 <= auc <= 1.0
    (m,) = M.metricas_por_umbral(predicciones, "prob", [0.5])
    assert m["vp"] + m["fn"] == datos.filter(f"{COLUMNA_ETIQUETA} = 1").count()


@pytest.mark.parametrize("algoritmo", modelos.ALGORITMOS)
def test_pipeline_sklearn_entrena_y_predice(oro_grande, algoritmo):
    pytest.importorskip("sklearn")
    pytest.importorskip("pandas")
    cats, nums = M.variables_de_variante("sin_plazo")
    pdf = oro_grande.select(*cats, *nums, COLUMNA_ETIQUETA).toPandas()  # 400 filas sintéticas
    x, y = pdf[cats + nums], pdf[COLUMNA_ETIQUETA]
    modelo = modelos.pipeline_sklearn(cats, nums, algoritmo).fit(x, y)
    prob = modelo.predict_proba(x)[:, 1]
    umbral = modelos.umbral_optimo_sklearn(y, prob, M.UMBRALES_POR_DEFECTO)
    metricas = modelos.metricas_sklearn(y, prob, umbral)
    assert set(metricas) == {"auc_roc", "auc_pr", "f1", "precision", "sensibilidad", "exactitud"}
    assert 0.0 <= metricas["auc_roc"] <= 1.0


def test_algoritmo_desconocido():
    with pytest.raises(ValueError):
        modelos.pipeline_mllib(["a"], ["b"], "red_neuronal")
