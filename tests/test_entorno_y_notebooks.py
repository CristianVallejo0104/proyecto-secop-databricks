"""Pruebas del entorno local, los gráficos y la higiene de los notebooks."""

import json
from pathlib import Path

import pytest

from secop import config, entorno

RAIZ = Path(__file__).resolve().parents[1]
NOTEBOOKS = sorted((RAIZ / "notebooks").glob("*.ipynb"))


def test_parametro_lee_variable_de_entorno(monkeypatch):
    assert entorno.parametro("NO_EXISTE_XYZ", "defecto") == "defecto"
    monkeypatch.setenv("SECOP_ANIOS", "2021")
    assert entorno.parametro("anios", "2019") == "2021"


def test_rutas_respetan_secop_datos(monkeypatch, tmp_path):
    monkeypatch.setenv("SECOP_DATOS", str(tmp_path))
    assert config.ruta_anio(2019) == str(tmp_path / "raw" / "contratos" / "anio=2019")
    assert config.ruta_tabla("contrato") == str(tmp_path / "delta" / "contrato")
    assert config.ruta_mlflow() == tmp_path / "mlruns"


def test_datos_por_defecto_estan_ignorados_por_git(monkeypatch):
    monkeypatch.delenv("SECOP_DATOS", raising=False)
    assert config.raiz_datos() == config.RAIZ_REPO / "datos"
    assert "datos/" in (RAIZ / ".gitignore").read_text(encoding="utf-8").splitlines()


def test_tabla_inexistente_da_error_claro(monkeypatch, tmp_path):
    monkeypatch.setenv("SECOP_DATOS", str(tmp_path))
    assert not entorno.tabla_existe("bronze_contratos")
    with pytest.raises(FileNotFoundError, match="Ejecuta antes"):
        entorno.leer_tabla(spark=None, nombre="bronze_contratos")


def test_version_git_devuelve_commit():
    version = entorno.version_git()
    assert set(version) == {"commit_git", "git_con_cambios_sin_commit"}
    assert len(version["commit_git"]) in (40, len("desconocido"))


def test_mostrar_trae_solo_las_filas_pedidas(spark, capsys):
    df = spark.range(1000)
    entorno.mostrar(df, filas=3)
    salida = capsys.readouterr().out
    assert "999" not in salida  # no trajo la tabla completa


def test_graficos_se_construyen():
    pytest.importorskip("matplotlib")
    import matplotlib

    matplotlib.use("Agg")  # sin pantalla
    import pandas as pd

    from secop import graficos

    pdf = pd.DataFrame({"tipo": ["Obra", "Compraventa"], "tasa_prorroga_pct": [20.0, 5.0]})
    figura = graficos.barras_tasa(pdf, "tipo", 11.0, "Prueba")
    assert figura.axes[0].get_title(loc="left") == "Prueba"
    linea = graficos.linea_tasa(pdf.rename(columns={"tipo": "periodo"}), "periodo", "Línea")
    assert len(linea.axes[0].lines) >= 1


@pytest.mark.parametrize("ruta", NOTEBOOKS, ids=lambda p: p.name)
def test_notebooks_se_suben_sin_salidas(ruta):
    """Un notebook con salidas podría llevar datos reales a GitHub: deben ir limpios."""
    nb = json.loads(ruta.read_text(encoding="utf-8"))
    for celda in nb["cells"]:
        if celda["cell_type"] == "code":
            assert celda.get("outputs") == [], f"{ruta.name} tiene salidas guardadas"
            assert celda.get("execution_count") is None, f"{ruta.name} tiene contadores"


@pytest.mark.parametrize("ruta", NOTEBOOKS, ids=lambda p: p.name)
def test_notebooks_no_usan_apis_prohibidas(ruta):
    codigo = "\n".join(
        "".join(c["source"])
        for c in json.loads(ruta.read_text(encoding="utf-8"))["cells"]
        if c["cell_type"] == "code"
    )
    for prohibido in ("dbutils", "display(", ".cache()", ".persist(", "sparkContext", ".rdd"):
        assert prohibido not in codigo, f"{ruta.name} usa {prohibido}"


def test_hay_seis_notebooks_en_orden():
    assert [p.name[:2] for p in NOTEBOOKS] == ["01", "02", "03", "04", "05", "06"]
