"""Pruebas de punta a punta de bronce -> plata -> oro con datos sintéticos.

Incluye pruebas de las formas normales (1FN, 2FN, 3FN) del modelo de plata.
"""

import pytest
from pyspark.sql import functions as F

from secop import plata
from secop.config import COLUMNA_ETIQUETA, VARIABLES_CATEGORICAS
from secop.esquema import COLUMNAS_BRONCE, COLUMNAS_POSTERIORES_FIRMA
from secop.oro import construir_oro_desde_plata
from secop.sinteticos import MARCADOR_PERSONAL
from secop.validacion import columnas_personales_presentes


@pytest.fixture(scope="module")
def tablas_plata(df_bronce):
    return plata.construir_plata(df_bronce)


@pytest.fixture(scope="module")
def df_oro(tablas_plata):
    return construir_oro_desde_plata(tablas_plata)


# ---------------------------------------------------------------------------
# Bronce y privacidad
# ---------------------------------------------------------------------------
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
        if len(cadenas) > 1:
            condicion = F.greatest(*[F.col(c) == MARCADOR_PERSONAL for c in cadenas])
        else:
            condicion = F.col(cadenas[0]) == MARCADOR_PERSONAL
        assert df.filter(condicion).count() == 0, nombre


# ---------------------------------------------------------------------------
# Modelo relacional: llaves e integridad
# ---------------------------------------------------------------------------
def test_se_construyen_las_10_tablas(tablas_plata):
    assert set(tablas_plata) == set(plata.LLAVES_PRIMARIAS)
    assert len(tablas_plata) == 10


@pytest.mark.parametrize("tabla", list(plata.LLAVES_PRIMARIAS))
def test_llave_primaria_unica_y_no_nula(tablas_plata, tabla):
    llave = plata.LLAVES_PRIMARIAS[tabla]
    df = tablas_plata[tabla]
    assert df.count() == df.select(*llave).distinct().count(), tabla
    nulos = F.lit(False)
    for c in llave:
        nulos = nulos | F.col(c).isNull()
    assert df.filter(nulos).count() == 0, tabla


@pytest.mark.parametrize("hijo, padre, llave", plata.RELACIONES)
def test_integridad_referencial_es_cero(tablas_plata, hijo, padre, llave):
    assert plata.filas_huerfanas(tablas_plata[hijo], tablas_plata[padre], llave).count() == 0


def test_filas_huerfanas_detecta_referencias_rotas(spark):
    hijo = spark.createDataFrame([("a",), ("z",), (None,)], "k string")
    padre = spark.createDataFrame([("a",)], "k string")
    assert [r.k for r in plata.filas_huerfanas(hijo, padre, "k").collect()] == ["z"]


def test_filas_huerfanas_con_llave_compuesta(spark):
    hijo = spark.createDataFrame([("A", "x"), ("B", "x"), ("A", None)], "d string, m string")
    padre = spark.createDataFrame([("A", "x")], "d string, m string")
    assert plata.filas_huerfanas(hijo, padre, ["d", "m"]).count() == 1  # solo ("B", "x")


def test_contratos_unicos_quita_duplicados(df_bronce):
    duplicado = df_bronce.limit(1).withColumn("ultima_actualizacion", F.lit("2030-01-01T00:00:00"))
    unicos = plata.contratos_unicos(df_bronce.unionByName(duplicado))
    assert unicos.count() == df_bronce.count()
    id_dup = duplicado.first()["id_contrato"]
    fila = unicos.filter(F.col("id_contrato") == id_dup).first()
    assert fila["ultima_actualizacion"].startswith("2030")


# ---------------------------------------------------------------------------
# Formas normales
# ---------------------------------------------------------------------------
def test_1fn_fuentes_en_formato_largo(tablas_plata):
    """1FN: sin grupos repetidos. Las 6 columnas de fuentes pasan a filas."""
    fuente = tablas_plata["contrato_fuente_recurso"]
    assert fuente.columns == ["id_contrato", "fuente", "valor"]
    assert fuente.filter("valor <= 0").count() == 0
    fuentes = {r.fuente for r in fuente.select("fuente").distinct().collect()}
    assert fuentes <= {"pgn", "sgp", "regalias", "propios_territoriales", "credito", "propios"}


def test_1fn_valores_atomicos(tablas_plata):
    """1FN: ni localización ni duración quedan como texto compuesto."""
    contrato = set(tablas_plata["contrato"].columns)
    entidad = set(tablas_plata["dim_entidad"].columns)
    assert {"duracion_numero", "duracion_unidad"} <= contrato
    assert "localizaci_n" not in entidad and {"departamento", "municipio"} <= entidad


def test_2fn_municipio_homonimo_requiere_llave_compuesta(tablas_plata):
    """2FN: 'Barbosa' existe en dos departamentos; el nombre solo no es llave."""
    barbosa = tablas_plata["dim_municipio"].filter("municipio = 'Barbosa'")
    assert {r.departamento for r in barbosa.collect()} == {"Antioquia", "Santander"}


def test_3fn_sin_dependencias_transitivas(tablas_plata):
    """3FN: cada atributo está en la tabla de la llave de la que depende."""
    columnas = {n: set(df.columns) for n, df in tablas_plata.items()}
    # contrato no repite atributos de proceso, entidad, proveedor ni categoría
    assert not columnas["contrato"] & {
        "modalidad_de_contratacion",
        "justificacion_modalidad",
        "es_grupo",
        "orden_entidad",
        "familia_unspsc",
        "segmento_unspsc",
    }
    # el segmento depende de la familia, no del código de categoría
    assert "segmento_unspsc" not in columnas["dim_categoria"]
    assert "segmento_unspsc" in columnas["dim_familia_unspsc"]
    # el país depende del departamento, no de la entidad ni del municipio
    assert "pais" not in columnas["dim_entidad"] | columnas["dim_municipio"]
    assert "pais" in columnas["dim_departamento"]


def test_3fn_sin_datos_calculados_en_plata(tablas_plata):
    """Los días de duración se calculan en oro, no se guardan en plata."""
    for nombre, df in tablas_plata.items():
        assert "duracion_dias_declarada" not in df.columns, nombre


def test_familia_y_segmento(tablas_plata):
    fila = tablas_plata["dim_familia_unspsc"].filter("familia_unspsc = '8011'").first()
    assert fila["segmento_unspsc"] == "80"


def test_entidad_con_dos_nombres_queda_con_el_completo(tablas_plata):
    fila = tablas_plata["dim_entidad"].filter("codigo_entidad = 'E900'").first()
    assert fila["nombre_entidad"] == "Ministerio de Pruebas Sintéticas"
    assert fila["n_nombres"] == 2


def test_proceso_con_dos_modalidades_se_marca(tablas_plata):
    fila = tablas_plata["dim_proceso"].filter("proceso_de_compra = 'PROC-DUPLICADO'").first()
    assert fila["n_modalidades"] == 2
    assert fila["modalidad_de_contratacion"] == "Licitación pública"  # la más reciente


def test_revisar_dependencias_cuenta_violaciones(df_bronce):
    resultado = {
        r.dependencia: r.violaciones for r in plata.revisar_dependencias(df_bronce).collect()
    }
    assert set(resultado) == {d[0] for d in plata.DEPENDENCIAS_FUNCIONALES}
    assert resultado["entidad -> nombre"] == 1  # E900 con nombre completo y sigla
    assert resultado["proceso -> modalidad"] == 1  # PROC-DUPLICADO
    assert resultado["familia -> segmento"] == 0
    assert resultado["proveedor -> tipo de documento"] == 0
    assert resultado["departamento -> pais"] == 0


# ---------------------------------------------------------------------------
# Separación "al firmar" / "después"
# ---------------------------------------------------------------------------
def test_contrato_no_tiene_informacion_posterior(tablas_plata):
    columnas = set(tablas_plata["contrato"].columns)
    assert not columnas & set(COLUMNAS_POSTERIORES_FIRMA)


def test_estado_limpio_en_ejecucion(tablas_plata):
    estados = {r[0] for r in tablas_plata["contrato_ejecucion"].select("estado_contrato").collect()}
    assert "EN EJECUCIÓN" not in estados and "En Ejecución" in estados


# ---------------------------------------------------------------------------
# Oro
# ---------------------------------------------------------------------------
def test_oro_sin_fuga_y_con_etiqueta_binaria(df_oro):
    assert not set(df_oro.columns) & set(COLUMNAS_POSTERIORES_FIRMA)
    etiquetas = {r[0] for r in df_oro.select(COLUMNA_ETIQUETA).distinct().collect()}
    assert etiquetas <= {0, 1} and 1 in etiquetas


def test_oro_una_fila_por_contrato(df_oro):
    assert df_oro.count() == df_oro.select("id_contrato").distinct().count()


def test_oro_filtra_valores_invalidos_y_fechas_nulas(df_oro, df_bronce):
    assert df_oro.filter("fecha_firma IS NULL").count() == 0
    for caso in ("CO1.PCCNTR.0000000", "CO1.PCCNTR.0000001", "CO1.PCCNTR.0000003"):
        assert df_oro.filter(F.col("id_contrato") == caso).count() == 0  # valor <= 0 o texto
    # Fila 17: días adicionados vacíos -> sin etiqueta -> fuera de oro
    assert df_oro.filter(F.col("id_contrato") == "CO1.PCCNTR.0000017").count() == 0
    assert df_oro.count() < df_bronce.count()


def test_oro_recupera_atributos_de_las_dimensiones(df_oro):
    fila = df_oro.filter(F.col("id_contrato") == "CO1.PCCNTR.0000022").first()
    assert fila["modalidad_de_contratacion"] == "Licitación pública"  # viene de dim_proceso
    assert fila["segmento_unspsc"] == fila["familia_unspsc"][:2]  # viene de dim_familia


def test_oro_calcula_duracion_en_dias(df_oro):
    nulos_o_positivos = df_oro.filter(
        F.col("duracion_dias_declarada").isNull() | (F.col("duracion_dias_declarada") > 0)
    )
    assert nulos_o_positivos.count() == df_oro.count()


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
