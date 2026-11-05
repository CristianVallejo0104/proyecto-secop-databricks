"""Pruebas de punta a punta de bronce -> plata -> oro con datos sintéticos."""

import pytest
from pyspark.sql import functions as F

from secop import plata
from secop.config import COLUMNA_ETIQUETA, VARIABLES_CATEGORICAS
from secop.esquema import COLUMNAS_BRONCE, COLUMNAS_POSTERIORES_FIRMA
from secop.oro import construir_oro
from secop.sinteticos import MARCADOR_PERSONAL
from secop.validacion import columnas_personales_presentes


@pytest.fixture(scope="module")
def tablas_plata(df_bronce):
    return {
        "entidad": plata.construir_dim_entidad(df_bronce),
        "proveedor": plata.construir_dim_proveedor(df_bronce),
        "categoria": plata.construir_dim_categoria(df_bronce),
        "contrato": plata.construir_contrato(df_bronce),
        "fuente": plata.construir_fuente_recurso(df_bronce),
        "ejecucion": plata.construir_ejecucion(df_bronce),
    }


@pytest.fixture(scope="module")
def df_oro(tablas_plata):
    t = tablas_plata
    return construir_oro(
        t["contrato"], t["entidad"], t["proveedor"], t["categoria"], t["ejecucion"]
    )


def test_bronce_sin_columnas_personales(df_bronce):
    assert columnas_personales_presentes(df_bronce.columns) == []
    assert df_bronce.columns == COLUMNAS_BRONCE + ["archivo_origen", "anio_carga"]
    assert dict(df_bronce.dtypes)["anio_carga"] == "int"


def test_bronce_falla_si_falta_una_columna(df_crudo):
    from secop.bronce import preparar_bronce

    with pytest.raises(ValueError, match="Faltan columnas"):
        preparar_bronce(df_crudo.drop("id_contrato"), 2019)


def test_ninguna_tabla_contiene_el_marcador_personal(tablas_plata, df_oro):
    for nombre, df in {**tablas_plata, "oro": df_oro}.items():
        assert columnas_personales_presentes(df.columns) == [], nombre
        cadenas = [c for c, t in df.dtypes if t == "string"]
        if cadenas:
            hay = df.filter(F.greatest(*[F.col(c) == MARCADOR_PERSONAL for c in cadenas])).count()
            assert hay == 0, nombre


def test_entidad_con_dos_nombres_queda_con_el_completo(tablas_plata):
    fila = tablas_plata["entidad"].filter("codigo_entidad = 'E900'").first()
    assert fila["nombre_entidad"] == "Ministerio de Pruebas Sintéticas"
    assert fila["n_nombres"] == 2


def test_dimensiones_tienen_llave_unica(tablas_plata):
    for nombre, llave in [
        ("entidad", "codigo_entidad"),
        ("proveedor", "codigo_proveedor"),
        ("categoria", "codigo_categoria"),
        ("contrato", "id_contrato"),
        ("ejecucion", "id_contrato"),
    ]:
        df = tablas_plata[nombre]
        assert df.count() == df.select(llave).distinct().count(), nombre


def test_contratos_unicos_quita_duplicados(spark, df_bronce):
    duplicado = df_bronce.limit(1).withColumn("ultima_actualizacion", F.lit("2030-01-01T00:00:00"))
    unicos = plata.contratos_unicos(df_bronce.unionByName(duplicado))
    assert unicos.count() == df_bronce.count()
    id_dup = duplicado.first()["id_contrato"]
    assert (
        unicos.filter(F.col("id_contrato") == id_dup)
        .first()["ultima_actualizacion"]
        .startswith("2030")
    )


def test_integridad_referencial_es_cero(tablas_plata):
    t = tablas_plata
    for hijo, padre, llave in [
        ("contrato", "entidad", "codigo_entidad"),
        ("contrato", "proveedor", "codigo_proveedor"),
        ("contrato", "categoria", "codigo_categoria"),
        ("fuente", "contrato", "id_contrato"),
        ("ejecucion", "contrato", "id_contrato"),
    ]:
        assert plata.filas_huerfanas(t[hijo], t[padre], llave).count() == 0, (hijo, padre)


def test_filas_huerfanas_detecta_referencias_rotas(spark):
    hijo = spark.createDataFrame([("a",), ("z",), (None,)], "k string")
    padre = spark.createDataFrame([("a",)], "k string")
    assert [r.k for r in plata.filas_huerfanas(hijo, padre, "k").collect()] == ["z"]


def test_fuentes_apiladas_solo_positivas(tablas_plata):
    fuente = tablas_plata["fuente"]
    assert fuente.columns == ["id_contrato", "fuente", "valor"]
    assert fuente.filter("valor <= 0").count() == 0
    fuentes = {r.fuente for r in fuente.select("fuente").distinct().collect()}
    assert fuentes <= {"pgn", "sgp", "regalias", "propios_territoriales", "credito", "propios"}


def test_contrato_no_tiene_informacion_posterior(tablas_plata):
    columnas = set(tablas_plata["contrato"].columns)
    assert not columnas & set(COLUMNAS_POSTERIORES_FIRMA)


def test_estado_limpio_en_ejecucion(tablas_plata):
    estados = {r[0] for r in tablas_plata["ejecucion"].select("estado_contrato").collect()}
    assert "EN EJECUCIÓN" not in estados and "En Ejecución" in estados


def test_oro_sin_fuga_y_con_etiqueta_binaria(df_oro):
    assert not set(df_oro.columns) & set(COLUMNAS_POSTERIORES_FIRMA)
    etiquetas = {r[0] for r in df_oro.select(COLUMNA_ETIQUETA).distinct().collect()}
    assert etiquetas <= {0, 1} and 1 in etiquetas


def test_oro_filtra_valores_invalidos_y_fechas_nulas(df_oro, df_bronce):
    assert df_oro.filter("fecha_firma IS NULL").count() == 0
    for caso in ("CO1.PCCNTR.0000000", "CO1.PCCNTR.0000001", "CO1.PCCNTR.0000003"):
        assert df_oro.filter(F.col("id_contrato") == caso).count() == 0  # valor <= 0 o texto
    # Fila 17: días adicionados vacíos -> sin etiqueta -> fuera de oro
    assert df_oro.filter(F.col("id_contrato") == "CO1.PCCNTR.0000017").count() == 0
    assert df_oro.count() < df_bronce.count()


def test_oro_valor_absurdo_queda_recortado(df_oro):
    fila = df_oro.filter(F.col("id_contrato") == "CO1.PCCNTR.0000002").first()
    assert fila["log_valor_contrato"] == pytest.approx(21.5117, abs=1e-3)


def test_oro_inicio_antes_de_firma(df_oro):
    fila = df_oro.filter(F.col("id_contrato") == "CO1.PCCNTR.0000015").first()
    assert fila["inicio_antes_de_firma"] == 1 and fila["dias_firma_a_inicio"] == -9


def test_oro_marca_anios_incompletos(df_oro):
    fila = df_oro.filter(F.col("id_contrato") == "CO1.PCCNTR.0000018").first()
    assert fila["anio_firma"] == 2025 and fila["etiqueta_completa"] is False


def test_oro_categoricas_sin_nulos(df_oro):
    for c in VARIABLES_CATEGORICAS:
        assert df_oro.filter(F.col(c).isNull()).count() == 0, c
