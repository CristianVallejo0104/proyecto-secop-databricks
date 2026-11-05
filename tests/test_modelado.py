"""Pruebas de partición temporal, pesos de clase, métricas y descriptivos."""

import pytest

from secop import modelado as M
from secop.descriptivo import tasa_por_grupo


@pytest.fixture(scope="module")
def df_pred(spark):
    # (fecha, etiqueta, probabilidad, grupo)
    filas = [
        ("2020-01-10", 1, 0.9, "a"),
        ("2020-02-10", 0, 0.8, "a"),
        ("2020-03-10", 1, 0.4, "b"),
        ("2020-04-10", 0, 0.1, "b"),
        ("2020-05-10", 0, 0.2, "b"),
        ("2020-08-10", 0, 0.3, "b"),
    ]
    return spark.createDataFrame(
        filas, "f string, etiqueta int, prob double, grupo string"
    ).selectExpr("to_date(f) AS fecha_firma", "etiqueta", "prob", "grupo")


def test_particion_temporal_no_se_traslapa(df_pred):
    ent, val, pru = M.particion_temporal(df_pred, "2020-04-01", "2020-07-01")
    assert (ent.count(), val.count(), pru.count()) == (3, 2, 1)


def test_particion_temporal_valida_cortes(df_pred):
    with pytest.raises(ValueError):
        M.particion_temporal(df_pred, "2020-07-01", "2020-04-01")


def test_pesos_balanceados(df_pred):
    pesos = M.calcular_pesos(df_pred)
    # n=6, positivos=2, negativos=4 -> 6/(2*2)=1,5 y 6/(2*4)=0,75
    assert pesos == {0: 0.75, 1: 1.5}
    total = M.agregar_peso(df_pred, pesos).groupBy("etiqueta").sum("peso").collect()
    assert {r[0]: r[1] for r in total} == {0: 3.0, 1: 3.0}  # ambas clases pesan igual


def test_metricas_por_umbral(df_pred):
    (m,) = M.metricas_por_umbral(df_pred, "prob", [0.5])
    # Con umbral 0,5: predice 1 a 0,9 (VP) y 0,8 (FP); el 0,4 positivo es FN.
    assert (m["vp"], m["fp"], m["fn"]) == (1, 1, 1)
    assert m["precision"] == 0.5 and m["sensibilidad"] == 0.5 and m["f1"] == 0.5


def test_elegir_umbral_maximiza_f1(df_pred):
    metricas = M.metricas_por_umbral(df_pred, "prob", [0.35, 0.5, 0.85])
    assert M.elegir_umbral(metricas)["umbral"] == 0.35  # VP=2, FP=1 -> F1=0,8


def test_variantes(df_pred):
    _, num = M.variables_de_variante("sin_plazo")
    _, num_plazo = M.variables_de_variante("con_plazo")
    assert "plazo_dias" not in num and "plazo_dias" in num_plazo
    with pytest.raises(ValueError):
        M.variables_de_variante("no_existe")


def test_tasa_por_grupo(df_pred):
    filas = {r.grupo: r for r in tasa_por_grupo(df_pred, ["grupo"]).collect()}
    assert filas["a"].contratos == 2 and filas["a"].tasa_prorroga_pct == 50.0
    assert filas["b"].tasa_prorroga_pct == 25.0
    assert tasa_por_grupo(df_pred, ["grupo"], min_contratos=3).count() == 1
