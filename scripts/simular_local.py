"""Ejecuta los notebooks 02 a 06 EN LOCAL, con Delta y datos sintéticos.

Sirve para comprobar que los notebooks corren de principio a fin antes de
llevarlos a Databricks. NO usa datos reales y sus métricas no significan nada
(los datos sintéticos son aleatorios).

Cómo simula Databricks:
- Crea una sesión de Spark local con Delta Lake.
- Escribe CSV sintéticos en una carpeta temporal que hace de Volume.
- Cambia el catálogo "workspace" por el catálogo local "spark_catalog".
- Reemplaza ``dbutils.widgets`` y ``display`` por versiones mínimas.
- MLflow guarda los experimentos en una carpeta local.

Requisitos: Java 17+ y ``pip install -r requirements-simulacion.txt``.
Spark descarga los jars de Delta la primera vez (necesita internet). Si tu red
lo bloquea, descarga los jars a mano y pon su carpeta en SECOP_JARS_DELTA.

Uso:  python scripts/simular_local.py [auto|mllib|sklearn]
"""

import glob
import os
import shutil
import sys
import tempfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))

from secop import config  # noqa: E402
from secop.sinteticos import escribir_csv, generar_filas  # noqa: E402

NOTEBOOKS = ["02_bronce.py", "03_plata.py", "04_oro.py", "05_descriptivo.py", "06_modelo.py"]


class _Widgets:
    """Imitación mínima de dbutils.widgets: devuelve el valor por defecto o el indicado."""

    def __init__(self, valores: dict[str, str]):
        self._valores, self._actuales = valores, {}

    def text(self, nombre, defecto, *_):
        self._actuales[nombre] = self._valores.get(nombre, defecto)

    def dropdown(self, nombre, defecto, opciones, *_):
        valor = self._valores.get(nombre, defecto)
        assert valor in opciones, f"{valor} no es una opción de {nombre}"
        self._actuales[nombre] = valor

    def get(self, nombre):
        return self._actuales[nombre]


class _DBUtils:
    def __init__(self, valores: dict[str, str]):
        self.widgets = _Widgets(valores)


def _display(df):
    """En Databricks display() dibuja una tabla; aquí se imprimen las primeras filas."""
    df.show(10, truncate=40)


def _preparar_datos(base: Path) -> None:
    """CSV sintéticos repartidos por año de firma, en varias partes, como la descarga real."""
    filas = generar_filas(3000, semilla=1)
    for anio in (2019, 2020):
        del_anio = [f for f in filas if f["fecha_de_firma"][:4] == str(anio)]
        mitad = len(del_anio) // 2
        carpeta = base / "contratos" / f"anio={anio}"
        escribir_csv(carpeta / "mes_01_parte_000.csv", del_anio[:mitad])
        escribir_csv(carpeta / "mes_01_parte_001.csv", del_anio[mitad:])
    # Casos raros (firma vacía, 2025) en una carpeta aparte para que también lleguen a bronce.
    otros = [f for f in filas if f["fecha_de_firma"][:4] not in ("2019", "2020")]
    escribir_csv(base / "contratos" / "anio=2021" / "mes_01_parte_000.csv", otros)


def _apuntar_config_a_local(base: Path) -> None:
    """Cambia catálogo y rutas de config para que apunten a la simulación."""
    config.CATALOGO = "spark_catalog"
    config.RUTA_CRUDA = str(base / "contratos")
    config.ruta_anio = lambda anio: f"{config.RUTA_CRUDA}/anio={anio}"
    for atributo in dir(config):
        if atributo.startswith("TABLA_"):
            nombre = getattr(config, atributo).split(".")[-1]
            setattr(config, atributo, f"spark_catalog.{config.ESQUEMA}.{nombre}")


def _crear_spark(base: Path):
    from pyspark.sql import SparkSession

    constructor = (
        SparkSession.builder.master("local[2]")
        .appName("simulacion-secop")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", str(base / "warehouse"))
        .config("spark.sql.ansi.enabled", "true")  # como serverless
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.log.level", "ERROR")
        .config("spark.ui.showConsoleProgress", "false")
    )
    jars = os.environ.get("SECOP_JARS_DELTA")
    if jars:
        return constructor.config("spark.jars", ",".join(glob.glob(f"{jars}/*.jar"))).getOrCreate()
    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(constructor).getOrCreate()


def main() -> int:
    motor = sys.argv[1] if len(sys.argv) > 1 else "auto"
    base = Path(tempfile.mkdtemp(prefix="secop_sim_"))
    try:
        _preparar_datos(base)
        _apuntar_config_a_local(base)
        spark = _crear_spark(base)

        import mlflow

        mlflow.set_tracking_uri(f"file:{base / 'mlruns'}")
        parametros = {
            "02_bronce.py": {"anios": "2019,2020,2021"},
            "06_modelo.py": {"motor": motor, "iteraciones_gbt": "5"},
        }
        os.chdir(RAIZ / "notebooks")  # como en un Git folder: cwd = carpeta del notebook
        for nombre in NOTEBOOKS:
            print(f"\n{'=' * 25} {nombre} {'=' * 25}")
            entorno = {
                "spark": spark,
                "dbutils": _DBUtils(parametros.get(nombre, {})),
                "display": _display,
                "__name__": "__main__",
            }
            codigo = (RAIZ / "notebooks" / nombre).read_text(encoding="utf-8")
            exec(compile(codigo, nombre, "exec"), entorno)  # noqa: S102 - código propio del repo
        print("\nSimulación completa: los 5 notebooks se ejecutaron sin errores.")
        return 0
    finally:
        shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
