"""Pruebas en Python puro: esquema, generador sintético y auditoría de privacidad.

No necesitan Spark, así que corren incluso donde no hay Java.
"""

import csv
import subprocess
from pathlib import Path

from secop.esquema import (
    COLUMNAS_BRONCE,
    COLUMNAS_CRUDAS,
    COLUMNAS_PERSONALES,
    COLUMNAS_POSTERIORES_FIRMA,
    FUENTES_DE_RECURSOS,
)
from secop.sinteticos import CASOS_RAROS, escribir_csv, generar_filas
from secop.validacion import comparar_columnas

RAIZ = Path(__file__).resolve().parents[1]


def test_esquema_tiene_95_columnas_unicas():
    assert len(COLUMNAS_CRUDAS) == 95
    assert len(set(COLUMNAS_CRUDAS)) == 95


def test_listas_son_subconjuntos_del_esquema():
    crudas = set(COLUMNAS_CRUDAS)
    assert set(COLUMNAS_PERSONALES) <= crudas
    assert set(COLUMNAS_POSTERIORES_FIRMA) <= crudas
    assert set(FUENTES_DE_RECURSOS.values()) <= crudas
    assert len(COLUMNAS_BRONCE) == 95 - len(COLUMNAS_PERSONALES)


def test_generador_reproducible_y_con_casos_raros(tmp_path):
    a, b = generar_filas(30, semilla=7), generar_filas(30, semilla=7)
    assert a == b  # misma semilla -> mismos datos
    assert len(generar_filas(1)) == len(CASOS_RAROS)  # nunca menos que los casos raros
    ruta = escribir_csv(tmp_path / "s.csv", a)
    with ruta.open(encoding="utf-8") as f:
        encabezado = next(csv.reader(f))
    assert comparar_columnas(encabezado)["valido"]


def test_comparar_columnas_reporta_faltantes_y_sobrantes():
    r = comparar_columnas(["id_contrato", "nueva"], esperadas=["id_contrato", "fecha_de_firma"])
    assert r == {"valido": False, "faltantes": ["fecha_de_firma"], "sobrantes": ["nueva"]}


def test_nombres_de_columnas_personales_solo_en_esquema():
    """Auditoría: los nombres de columnas personales solo se escriben en esquema.py.

    Cualquier otro archivo debe referirse a ellas mediante COLUMNAS_PERSONALES.
    """
    permitidos = {"src/secop/esquema.py"}
    archivos = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=RAIZ,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    infractores = []
    for nombre in archivos:
        if nombre in permitidos:
            continue
        ruta = RAIZ / nombre
        if not ruta.is_file():
            continue
        texto = ruta.read_text(encoding="utf-8", errors="ignore")
        for columna in COLUMNAS_PERSONALES:
            if columna in texto:
                infractores.append((nombre, columna))
    assert infractores == []
