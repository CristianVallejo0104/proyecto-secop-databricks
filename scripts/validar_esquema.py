"""Valida el esquema del proyecto con datos SINTÉTICOS, sin Spark ni internet.

Es lo que ejecuta el contenedor Docker. Comprueba que:
1. El esquema crudo tiene 95 columnas únicas.
2. Un CSV sintético generado con ese esquema se lee con las mismas columnas.
3. Las columnas que van a bronce no incluyen ninguna columna personal.
4. Las variables del modelo no incluyen columnas posteriores a la firma.

Uso:  python scripts/validar_esquema.py
Sale con código 0 si todo está bien y 1 si algo falla (útil en CI y Docker).
"""

import csv
import sys
import tempfile
from pathlib import Path

# Permite "import secop" sin instalar el paquete: añade src/ a la ruta de búsqueda.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from secop import config  # noqa: E402
from secop.esquema import COLUMNAS_BRONCE, COLUMNAS_CRUDAS  # noqa: E402
from secop.sinteticos import escribir_csv, generar_filas  # noqa: E402
from secop.validacion import (  # noqa: E402
    columnas_personales_presentes,
    columnas_posteriores_presentes,
    comparar_columnas,
)


def main() -> int:
    errores = []

    # 1. Esquema crudo
    if len(COLUMNAS_CRUDAS) != 95 or len(set(COLUMNAS_CRUDAS)) != 95:
        errores.append("El esquema crudo no tiene 95 columnas únicas")

    # 2. CSV sintético de ida y vuelta
    with tempfile.TemporaryDirectory() as carpeta:
        ruta = escribir_csv(Path(carpeta) / "sintetico.csv", generar_filas(50))
        with ruta.open(encoding="utf-8") as f:
            lector = csv.reader(f)
            encabezado = next(lector)
            n_filas = sum(1 for _ in lector)
    resultado = comparar_columnas(encabezado)
    if not resultado["valido"]:
        errores.append(f"CSV sintético sin columnas: {resultado['faltantes']}")

    # 3. Bronce sin datos personales
    if columnas_personales_presentes(COLUMNAS_BRONCE):
        errores.append("Bronce incluiría columnas personales")

    # 4. Variables del modelo sin fuga
    variables = config.VARIABLES_CATEGORICAS + config.VARIABLES_NUMERICAS + config.VARIABLES_PLAZO
    if columnas_posteriores_presentes(variables):
        errores.append("Hay variables posteriores a la firma en el modelo")

    print(f"Columnas crudas: {len(COLUMNAS_CRUDAS)} | columnas en bronce: {len(COLUMNAS_BRONCE)}")
    print(f"CSV sintético: {n_filas} filas, {len(encabezado)} columnas")
    print(f"Variables del modelo: {len(variables)}")
    if errores:
        for e in errores:
            print("ERROR:", e)
        return 1
    print("Validación del esquema: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
