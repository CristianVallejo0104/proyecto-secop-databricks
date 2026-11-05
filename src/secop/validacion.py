"""Validaciones de esquema en Python puro (sin Spark).

Al no depender de Spark se pueden usar en cualquier lugar: dentro de los
notebooks (con ``df.columns``), en el contenedor Docker y en GitHub Actions.
"""

from secop.esquema import COLUMNAS_CRUDAS, COLUMNAS_PERSONALES, COLUMNAS_POSTERIORES_FIRMA


def comparar_columnas(columnas: list[str], esperadas: list[str] = COLUMNAS_CRUDAS) -> dict:
    """Compara las columnas recibidas contra las esperadas.

    Devuelve un diccionario con las columnas faltantes, las sobrantes y si el
    esquema es válido (no falta ninguna). Las sobrantes solo se reportan: si la
    fuente agrega una columna nueva, no es motivo para detener la carga.
    """
    recibidas = set(columnas)
    faltantes = [c for c in esperadas if c not in recibidas]
    sobrantes = sorted(recibidas - set(esperadas))
    return {"valido": not faltantes, "faltantes": faltantes, "sobrantes": sobrantes}


def exigir_columnas(columnas: list[str], esperadas: list[str] = COLUMNAS_CRUDAS) -> None:
    """Lanza un error claro si falta alguna columna esperada."""
    resultado = comparar_columnas(columnas, esperadas)
    if not resultado["valido"]:
        raise ValueError(f"Faltan columnas en la fuente: {resultado['faltantes']}")


def columnas_personales_presentes(columnas: list[str]) -> list[str]:
    """Columnas con datos personales que aparecen en ``columnas`` (debe ser [])."""
    return [c for c in columnas if c in COLUMNAS_PERSONALES]


def columnas_posteriores_presentes(columnas: list[str]) -> list[str]:
    """Columnas posteriores a la firma que aparecen en ``columnas`` (fuga)."""
    return [c for c in columnas if c in COLUMNAS_POSTERIORES_FIRMA]


def exigir_sin_personales(columnas: list[str]) -> None:
    """Detiene el proceso si una tabla de salida tiene datos personales."""
    presentes = columnas_personales_presentes(columnas)
    if presentes:
        raise ValueError(f"La tabla contiene columnas con datos personales: {presentes}")
