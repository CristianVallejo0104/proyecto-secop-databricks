"""Ejecuta los notebooks del pipeline de principio a fin, en orden.

Dos modos:

1. ``--sinteticos`` (pruebas): crea una carpeta temporal con CSV SINTÉTICOS y
   ejecuta los notebooks 02 a 06 contra ella. No usa internet ni datos reales; el
   01 se salta porque descarga de la API. Es lo que corre la CI en cada push.
   Las métricas que salen NO significan nada (los datos son aleatorios).

2. Sin ``--sinteticos`` (pipeline real): ejecuta 01 a 06 sobre ``datos/``.
   El 01 descarga lo que falte de la API (puede tardar).

Los notebooks del repositorio NO se modifican: se ejecuta una copia en memoria.
Con ``--guardar CARPETA`` se guardan las copias ejecutadas (con tablas, gráficos
y mensajes) en esa carpeta; por defecto ``datos/ejecutados/``, que está en
.gitignore, para que los resultados nunca se suban por accidente.

Uso:
    python scripts/ejecutar_notebooks.py --sinteticos
    python scripts/ejecutar_notebooks.py                  # pipeline real
    python scripts/ejecutar_notebooks.py --desde 03       # desde el notebook 03
    python scripts/ejecutar_notebooks.py --sinteticos --motor sklearn
"""

import argparse
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
CARPETA_NOTEBOOKS = RAIZ / "notebooks"
sys.path.insert(0, str(RAIZ / "src"))


def preparar_sinteticos(base: Path) -> None:
    """Escribe CSV sintéticos repartidos por año, en varias partes, como la descarga real."""
    from secop.sinteticos import escribir_csv, generar_filas

    filas = generar_filas(3000, semilla=1)
    for anio in (2019, 2020):
        del_anio = [f for f in filas if f["fecha_de_firma"][:4] == str(anio)]
        mitad = len(del_anio) // 2
        carpeta = base / "raw" / "contratos" / f"anio={anio}"
        escribir_csv(carpeta / "mes_01_parte_000.csv", del_anio[:mitad])
        escribir_csv(carpeta / "mes_01_parte_001.csv", del_anio[mitad:])
    # Casos raros (firma vacía, 2025) en otra carpeta para que también lleguen a bronce.
    otros = [f for f in filas if f["fecha_de_firma"][:4] not in ("2019", "2020")]
    escribir_csv(base / "raw" / "contratos" / "anio=2021" / "mes_01_parte_000.csv", otros)


def ejecutar(ruta: Path, guardar_en: Path | None) -> float:
    """Ejecuta un notebook con un kernel nuevo. Devuelve los segundos que tardó."""
    import nbformat
    from nbclient import NotebookClient

    inicio = time.time()
    nb = nbformat.read(ruta, as_version=4)
    # cwd = carpeta de notebooks, igual que al abrirlo en VS Code o Jupyter.
    # timeout: segundos máximos por celda. Evita que la CI espere para siempre si algo
    # se cuelga; con datos reales el modelo puede tardar, por eso se puede ampliar.
    cliente = NotebookClient(
        nb,
        timeout=int(os.environ.get("SECOP_TIMEOUT_CELDA", "1200")),
        kernel_name="python3",
        resources={"metadata": {"path": str(CARPETA_NOTEBOOKS)}},
    )
    try:
        cliente.execute()
    finally:
        if guardar_en is not None:
            guardar_en.mkdir(parents=True, exist_ok=True)
            nbformat.write(nb, guardar_en / ruta.name)
    return time.time() - inicio


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--sinteticos", action="store_true", help="usar datos sintéticos")
    parser.add_argument("--desde", default="01", help="número del primer notebook (01-06)")
    parser.add_argument("--motor", choices=["auto", "mllib", "sklearn"], help="motor de 06")
    parser.add_argument("--guardar", help="carpeta para las copias ejecutadas")
    args = parser.parse_args()

    temporal = None
    if args.sinteticos:
        temporal = Path(tempfile.mkdtemp(prefix="secop_sinteticos_"))
        preparar_sinteticos(temporal)
        os.environ["SECOP_DATOS"] = str(temporal)  # el kernel hereda las variables
        os.environ["SECOP_ANIOS"] = "2019,2020,2021"
        os.environ.setdefault("SECOP_ITER_GBT", "5")  # rápido: solo se prueba que corre
        desde = max(args.desde, "02")  # el 01 descarga de internet: se salta
    else:
        desde = args.desde
    if args.motor:
        os.environ["SECOP_MOTOR"] = args.motor

    from secop import config

    guardar_en = Path(args.guardar) if args.guardar else config.raiz_datos() / "ejecutados"
    if args.sinteticos and not args.guardar:
        guardar_en = None  # en modo prueba no se guarda nada

    notebooks = sorted(p for p in CARPETA_NOTEBOOKS.glob("*.ipynb") if p.name[:2] >= desde)
    try:
        for ruta in notebooks:
            print(f"==> {ruta.name} ...", flush=True)
            segundos = ejecutar(ruta, guardar_en)
            print(f"    listo en {segundos:.0f} s", flush=True)
        print(f"\nOK: {len(notebooks)} notebooks ejecutados sin errores.")
        if guardar_en is not None:
            print(f"Copias con resultados en: {guardar_en}")
        return 0
    finally:
        if temporal is not None:
            shutil.rmtree(temporal, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
