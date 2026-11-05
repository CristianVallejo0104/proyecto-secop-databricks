"""Pruebas de las funciones de limpieza, una por cada regla."""

import datetime as dt

import pytest
from pyspark.sql import functions as F

from secop import limpieza as L


def _evaluar(spark, valores, funcion):
    """Aplica ``funcion`` a una columna "x" con ``valores`` y devuelve la lista."""
    df = spark.createDataFrame([(v,) for v in valores], "x string")
    return [r[0] for r in df.select(funcion(F.col("x"))).collect()]


def test_a_numero_no_falla_con_texto(spark):
    assert _evaluar(spark, ["1500", " 2.5 ", "No definido", None], L.a_numero) == [
        1500.0,
        2.5,
        None,
        None,
    ]


def test_a_fecha_corta_los_primeros_10_caracteres(spark):
    resultado = _evaluar(spark, ["2019-03-15T00:00:00.000", "sin fecha", "", None], L.a_fecha)
    assert resultado == [dt.date(2019, 3, 15), None, None, None]


def test_limpiar_estado_unifica_mayusculas(spark):
    resultado = _evaluar(
        spark, ["EN EJECUCIÓN", "en ejecución", " En Ejecución "], L.limpiar_estado
    )
    assert set(resultado) == {"En Ejecución"}


@pytest.mark.parametrize("valor", ["No Definido", "no definido", "NO DEFINIDO", "", None])
def test_no_definido_se_vuelve_una_sola_categoria(spark, valor):
    assert _evaluar(spark, [valor], L.limpiar_categoria) == [L.CATEGORIA_SIN_DATO]


def test_valor_positivo_anula_ceros_y_negativos(spark):
    assert _evaluar(spark, ["0", "-5", "10", "abc"], L.valor_positivo) == [None, None, 10.0, None]


def test_valor_recortado_y_log(spark):
    def recorte(c):
        return L.valor_recortado(c, 2.2e9)

    def logaritmo(c):
        return L.log_valor(c, 2.2e9)

    assert _evaluar(spark, ["28000000000000000", "1000"], recorte) == [2.2e9, 1000.0]
    resultado = _evaluar(spark, ["28000000000000000"], logaritmo)
    assert resultado[0] == pytest.approx(21.5115, abs=1e-3)  # log(1 + 2,2e9)


def test_separar_localizacion(spark):
    df = spark.createDataFrame(
        [("Colombia, Antioquia, Medellín",), ("Colombia",), (None,)], "x string"
    )
    filas = df.withColumns(L.separar_localizacion("x")).collect()
    assert (filas[0]["pais"], filas[0]["departamento_loc"], filas[0]["ciudad_loc"]) == (
        "Colombia",
        "Antioquia",
        "Medellín",
    )
    assert (filas[1]["pais"], filas[1]["departamento_loc"]) == ("Colombia", None)
    assert filas[2]["pais"] is None


@pytest.mark.parametrize(
    "texto, numero, unidad, dias",
    [
        ("6 Mes(es)", 6.0, "mes", 180.0),
        ("30 Dia(s)", 30.0, "dia", 30.0),
        ("2 Semana(s)", 2.0, "semana", 14.0),
        ("1 Año(s)", 1.0, "anio", 365.0),
        ("Mes(es)", None, None, None),
        ("No definido", None, None, None),
        (None, None, None, None),
    ],
)
def test_separar_duracion(spark, texto, numero, unidad, dias):
    df = spark.createDataFrame([(texto,)], "x string")
    fila = (
        df.withColumns(L.separar_duracion("x"))
        .withColumn(
            "duracion_dias_declarada",
            L.dias_desde_duracion("duracion_numero", "duracion_unidad"),
        )
        .first()
    )
    assert fila["duracion_numero"] == numero
    assert fila["duracion_unidad"] == unidad
    assert fila["duracion_dias_declarada"] == dias


def test_familia_y_segmento_unspsc(spark):
    assert _evaluar(spark, ["V1.80111600", "80111600", "No definido"], L.familia_unspsc) == [
        "8011",
        "8011",
        None,
    ]
    assert _evaluar(spark, ["V1.80111600"], L.segmento_unspsc) == ["80"]
