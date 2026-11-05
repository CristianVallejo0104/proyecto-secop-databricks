"""Funciones puras de limpieza de columnas.

"Pura" significa: recibe columnas o DataFrames y devuelve columnas o DataFrames
nuevos, sin escribir tablas ni leer variables globales. Los DataFrames de Spark
son inmutables: cada función devuelve un objeto nuevo y no modifica el original.

Todas las conversiones usan ``try_cast``: si el texto no se puede convertir,
devuelve NULL en lugar de detener el trabajo. Esto importa porque Databricks
serverless trabaja en modo ANSI, donde un ``cast`` normal falla con el primer
valor inválido (por ejemplo "No definido" en una columna numérica).
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

# Textos que la fuente usa para decir "no hay información".
TEXTOS_NO_DEFINIDO = ("no definido", "no definida", "no aplica", "n/a", "na", "null", "")

# Categoría que se asigna cuando el valor falta o es "No Definido".
CATEGORIA_SIN_DATO = "No Definido"


def _col(c: str | Column) -> Column:
    """Acepta un nombre de columna o una columna y siempre devuelve una columna."""
    return F.col(c) if isinstance(c, str) else c


# ---------------------------------------------------------------------------
# Tipos
# ---------------------------------------------------------------------------
def a_numero(c: str | Column) -> Column:
    """Convierte texto a número decimal (double) de forma segura.

    "1500000"   -> 1500000.0
    " 1500.5 "  -> 1500.5
    "No definido" o NULL -> NULL
    """
    # trim quita espacios; try_cast devuelve NULL si el texto no es un número.
    return F.trim(_col(c)).try_cast("double")


def a_entero(c: str | Column) -> Column:
    """Convierte texto a entero largo de forma segura (NULL si no se puede).

    Se pasa primero por double para aceptar textos como "15.0".
    """
    return a_numero(c).try_cast("long")


def a_fecha(c: str | Column) -> Column:
    """Convierte un texto de fecha/hora de la API a tipo fecha.

    La API entrega "2019-03-15T00:00:00.000". Los primeros 10 caracteres
    (posición 1 a 10) son siempre AAAA-MM-DD, así que se cortan y se convierten.
    Cualquier otro formato o texto vacío queda NULL.
    """
    # substr(1, 10): Spark cuenta desde 1. "2019-03-15T00:00:00.000" -> "2019-03-15"
    return F.substring(F.trim(_col(c)), 1, 10).try_cast("date")


# ---------------------------------------------------------------------------
# Texto y categorías
# ---------------------------------------------------------------------------
def es_no_definido(c: str | Column) -> Column:
    """Verdadero si el valor es NULL, vacío o una variante de "No Definido"."""
    # lower+trim unifica "No Definido", "NO DEFINIDO", " no definido ".
    texto = F.lower(F.trim(_col(c)))
    return texto.isNull() | texto.isin(*TEXTOS_NO_DEFINIDO)


def limpiar_categoria(c: str | Column) -> Column:
    """Normaliza una variable categórica.

    - Quita espacios sobrantes.
    - NULL, vacío y variantes de "No Definido" -> "No Definido" (una sola categoría).
    Se conserva "No Definido" como categoría propia (en lugar de NULL) porque
    puede ser informativa: en 2019 ~71 % de habilita_pago_adelantado la tiene.
    """
    return F.when(es_no_definido(c), F.lit(CATEGORIA_SIN_DATO)).otherwise(F.trim(_col(c)))


def limpiar_estado(c: str | Column) -> Column:
    """Unifica mayúsculas del estado: "EN EJECUCIÓN", "en ejecución" -> "En Ejecución".

    initcap pone en mayúscula la primera letra de cada palabra.
    """
    return F.when(es_no_definido(c), F.lit(CATEGORIA_SIN_DATO)).otherwise(
        F.initcap(F.trim(_col(c)))
    )


def texto_o_nulo(c: str | Column) -> Column:
    """Devuelve el texto sin espacios, o NULL si es vacío/"No Definido".

    Se usa para llaves (códigos) donde "No Definido" no debe ser un valor válido.
    """
    return F.when(es_no_definido(c), F.lit(None).cast("string")).otherwise(F.trim(_col(c)))


# ---------------------------------------------------------------------------
# Valores monetarios
# ---------------------------------------------------------------------------
def valor_positivo(c: str | Column) -> Column:
    """Valor en pesos como double; los valores <= 0 se vuelven NULL.

    Un contrato con valor 0 o negativo es un error de registro (833 casos en 2019),
    no un contrato gratuito, así que se trata como dato faltante.
    """
    v = a_numero(c)
    return F.when(v > 0, v)  # sin otherwise -> NULL cuando v <= 0 o v es NULL


def valor_recortado(c: str | Column, tope: float) -> Column:
    """Valor positivo recortado en ``tope`` (winsorización superior).

    least(v, tope) devuelve el menor de los dos: un valor absurdo como 2,8e16
    queda en el tope en lugar de distorsionar promedios y el modelo.
    """
    return F.least(valor_positivo(c), F.lit(float(tope)))


def log_valor(c: str | Column, tope: float) -> Column:
    """log(1 + valor recortado). Comprime la escala: 1e6 -> 13,8; 1e9 -> 20,7.

    Los valores monetarios son muy asimétricos; el logaritmo los vuelve
    comparables y ayuda sobre todo a la regresión logística.
    """
    return F.log1p(valor_recortado(c, tope))


# ---------------------------------------------------------------------------
# Columnas compuestas
# ---------------------------------------------------------------------------
def separar_localizacion(c: str | Column) -> dict[str, Column]:
    """Separa "País, Departamento, Ciudad" en tres columnas.

    Devuelve un diccionario {nombre: columna} para usarlo con ``withColumns``.
    ``F.get`` devuelve NULL si la posición no existe (en lugar de fallar),
    así "Colombia" sola deja departamento y ciudad en NULL.
    """
    # split con la expresión regular ",\s*" corta en cada coma y quita el espacio.
    partes = F.split(F.trim(_col(c)), r"\s*,\s*")
    return {
        "pais": texto_o_nulo(F.get(partes, 0)),
        "departamento_loc": texto_o_nulo(F.get(partes, 1)),
        "ciudad_loc": texto_o_nulo(F.get(partes, 2)),
    }


# Días aproximados por unidad de duración.
DIAS_POR_UNIDAD = {"dia": 1.0, "semana": 7.0, "mes": 30.0, "anio": 365.0, "hora": 1.0 / 24.0}


def separar_duracion(c: str | Column) -> dict[str, Column]:
    """Separa "6 Mes(es)" en número (6) y unidad ("mes"). Esto es 1FN: valores atómicos.

    Formatos vistos: "6 Mes(es)", "30 Dia(s)", "1 Año(s)", "No definido".
    - duracion_numero: los dígitos iniciales; NULL si no hay número.
    - duracion_unidad: según el inicio de la palabra (dia/día, sem, mes, año/ano, hora).
    Los días aproximados NO se guardan aquí (serían un dato calculado, redundante
    en una tabla normalizada): se calculan en oro con ``dias_desde_duracion``.
    """
    texto = F.lower(F.trim(_col(c)))
    # regexp_extract devuelve "" si no hay coincidencia; try_cast("") -> NULL.
    numero = F.regexp_extract(texto, r"^(\d+(?:[.,]\d+)?)", 1)
    numero = F.regexp_replace(numero, ",", ".").try_cast("double")
    # La unidad es la primera palabra después del número.
    palabra = F.regexp_extract(texto, r"^\d+(?:[.,]\d+)?\s*([a-záéíóúñ]+)", 1)
    unidad = (
        F.when(palabra.rlike("^d[ií]a"), "dia")
        .when(palabra.startswith("sem"), "semana")
        .when(palabra.startswith("mes"), "mes")
        .when(palabra.rlike("^a[ñn]o"), "anio")
        .when(palabra.startswith("hora"), "hora")
    )
    return {"duracion_numero": numero, "duracion_unidad": unidad}


def dias_desde_duracion(numero: str | Column, unidad: str | Column) -> Column:
    """Días aproximados = número x días por unidad (mes = 30, año = 365...).

    NULL si falta el número o la unidad. Ejemplo: (6, "mes") -> 180.
    """
    u = _col(unidad)
    factor = F.when(u == "dia", DIAS_POR_UNIDAD["dia"])
    for nombre in ("semana", "mes", "anio", "hora"):
        factor = factor.when(u == nombre, DIAS_POR_UNIDAD[nombre])
    return _col(numero) * factor


def familia_unspsc(c: str | Column) -> Column:
    """Extrae la familia UNSPSC (4 dígitos) de un código como "V1.80111600".

    El código UNSPSC tiene 8 dígitos: segmento(2) familia(2) clase(2) producto(2).
    La expresión (\\d{4})\\d{4}$ toma los 4 primeros de los últimos 8 dígitos:
    "V1.80111600" -> "8011" (segmento 80, familia 11). Si no hay 8 dígitos al
    final, devuelve NULL.
    """
    familia = F.regexp_extract(F.trim(_col(c)), r"(\d{4})\d{4}$", 1)
    return F.when(familia != "", familia)


def segmento_unspsc(c: str | Column) -> Column:
    """Segmento UNSPSC (2 dígitos), el nivel más general: "V1.80111600" -> "80"."""
    return F.substring(familia_unspsc(c), 1, 2)
