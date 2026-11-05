"""Generador de datos SINTÉTICOS con el esquema crudo de 95 columnas.

No contiene ni un dato real: todos los valores se inventan con una semilla fija
(siempre salen los mismos). Sirve para probar el código sin descargar nada.

Las primeras filas son CASOS RAROS fijos, elegidos a partir de los problemas
observados en 2019; el resto son filas "normales" aleatorias.

Solo usa la biblioteca estándar de Python (sin Spark ni pandas), para que se
pueda ejecutar también dentro del contenedor Docker.
"""

import csv
import random
from pathlib import Path

from secop.esquema import COLUMNAS_CRUDAS, COLUMNAS_PERSONALES, FUENTES_DE_RECURSOS

# Marcador que se pone en las columnas personales. Si alguna vez aparece en una
# tabla de salida, los tests lo detectan.
MARCADOR_PERSONAL = "DATO_PERSONAL_SINTETICO"

_TIPOS = ["Prestación de servicios", "Compraventa", "Suministros", "Obra", "Otro"]
_MODALIDADES = ["Contratación directa", "Mínima cuantía", "Licitación pública"]
_UNIDADES = ["Dia(s)", "Mes(es)", "Semana(s)", "Año(s)"]
_DEPTOS = [("Bogotá D.C.", "Bogotá"), ("Antioquia", "Medellín"), ("Valle del Cauca", "Cali")]


def _fila_base(i: int, rng: random.Random) -> dict[str, str]:
    """Una fila normal con todas las columnas como texto."""
    depto, ciudad = rng.choice(_DEPTOS)
    mes = rng.randint(1, 12)
    dia = rng.randint(1, 28)
    anio = rng.choice([2019, 2020])
    fila = {c: "" for c in COLUMNAS_CRUDAS}
    fila.update(
        {
            "nombre_entidad": f"Entidad Sintética {i % 5}",
            "nit_entidad": f"9000{i % 5:05d}",
            "codigo_entidad": f"E{i % 5:03d}",
            "departamento": depto,
            "ciudad": ciudad,
            "localizaci_n": f"Colombia, {depto}, {ciudad}",
            "orden": rng.choice(["Nacional", "Territorial"]),
            "sector": rng.choice(["Salud", "Educación", "Transporte"]),
            "rama": "Ejecutivo",
            "entidad_centralizada": rng.choice(["Centralizada", "Descentralizada"]),
            "proceso_de_compra": f"PROC-{i:06d}",
            "id_contrato": f"CO1.PCCNTR.{i:07d}",
            "referencia_del_contrato": f"REF-{i:06d}",
            "estado_contrato": rng.choice(["En ejecución", "Terminado", "cerrado"]),
            "codigo_de_categoria_principal": f"V1.{rng.choice(['8011', '4321', '7214'])}1600",
            "tipo_de_contrato": rng.choice(_TIPOS),
            "modalidad_de_contratacion": rng.choice(_MODALIDADES),
            "justificacion_modalidad_de": "Servicios profesionales",
            "fecha_de_firma": f"{anio}-{mes:02d}-{dia:02d}T00:00:00.000",
            "fecha_de_inicio_del_contrato": f"{anio}-{mes:02d}-{dia:02d}T00:00:00.000",
            "fecha_de_fin_del_contrato": f"{anio}-12-31T00:00:00.000",
            "condiciones_de_entrega": "A convenir",
            # El tipo de documento depende del proveedor (dependencia funcional real).
            "tipodocproveedor": "NIT" if (i % 7) % 2 == 0 else "Cédula de Ciudadanía",
            "codigo_proveedor": f"P{i % 7:05d}",
            "es_grupo": "No",
            "es_pyme": rng.choice(["Si", "No"]),
            "habilita_pago_adelantado": rng.choice(["Si", "No", "No Definido"]),
            "liquidaci_n": rng.choice(["Si", "No"]),
            "obligaci_n_ambiental": "No",
            "obligaciones_postconsumo": "No",
            "reversion": "No",
            "origen_de_los_recursos": "Distribuido",
            "destino_gasto": rng.choice(["Funcionamiento", "Inversión"]),
            "valor_del_contrato": str(rng.randint(1, 500) * 1_000_000),
            "valor_de_pago_adelantado": "0",
            "valor_facturado": str(rng.randint(0, 100) * 1_000_000),
            "valor_pagado": str(rng.randint(0, 100) * 1_000_000),
            "espostconflicto": "No",
            "dias_adicionados": rng.choice(["0", "0", "0", "0", "0", "0", "0", "30"]),
            "ultima_actualizacion": f"{anio + 1}-01-15T10:00:00.000",
            "duraci_n_del_contrato": f"{rng.randint(1, 12)} {rng.choice(_UNIDADES)}",
            "el_contrato_puede_ser_prorrogado": rng.choice(["Si", "No"]),
            "objeto_del_contrato": "Objeto sintético de prueba",
        }
    )
    for crudo in FUENTES_DE_RECURSOS.values():
        fila[crudo] = rng.choice(["0", "0", str(rng.randint(1, 50) * 1_000_000)])
    for c in COLUMNAS_PERSONALES:
        fila[c] = MARCADOR_PERSONAL
    return fila


# Casos raros: (descripción, cambios sobre una fila base)
CASOS_RAROS: list[tuple[str, dict[str, str]]] = [
    ("valor cero", {"valor_del_contrato": "0"}),
    ("valor negativo", {"valor_del_contrato": "-5000000"}),
    ("valor absurdo", {"valor_del_contrato": "28000000000000000"}),
    ("valor no numérico", {"valor_del_contrato": "No Definido"}),
    ("pago adelantado no definido", {"habilita_pago_adelantado": "No Definido"}),
    ("fecha de firma vacía", {"fecha_de_firma": ""}),
    ("fecha de inicio nula", {"fecha_de_inicio_del_contrato": ""}),
    ("fecha inválida", {"fecha_de_fin_del_contrato": "sin fecha"}),
    (
        "entidad con nombre completo",
        {"codigo_entidad": "E900", "nombre_entidad": "Ministerio de Pruebas Sintéticas"},
    ),
    ("misma entidad con sigla", {"codigo_entidad": "E900", "nombre_entidad": "MINPRUEBAS"}),
    ("duración sin número", {"duraci_n_del_contrato": "Mes(es)"}),
    ("duración no definida", {"duraci_n_del_contrato": "No definido"}),
    ("estado en mayúsculas", {"estado_contrato": "EN EJECUCIÓN"}),
    ("localización incompleta", {"localizaci_n": "Colombia", "departamento": ""}),
    ("categoría sin código", {"codigo_de_categoria_principal": "No definido"}),
    (
        "inicio antes de la firma",
        {
            "fecha_de_firma": "2019-05-10T00:00:00.000",
            "fecha_de_inicio_del_contrato": "2019-05-01T00:00:00.000",
        },
    ),
    ("con prórroga", {"dias_adicionados": "45"}),
    ("días adicionados vacíos", {"dias_adicionados": ""}),
    ("año con etiqueta incompleta", {"fecha_de_firma": "2025-03-01T00:00:00.000"}),
    # Municipio homónimo: mismo nombre en dos departamentos (llave compuesta).
    (
        "municipio homónimo en Antioquia",
        {
            "codigo_entidad": "E910",
            "departamento": "Antioquia",
            "ciudad": "Barbosa",
            "localizaci_n": "Colombia, Antioquia, Barbosa",
        },
    ),
    (
        "municipio homónimo en Santander",
        {
            "codigo_entidad": "E911",
            "departamento": "Santander",
            "ciudad": "Barbosa",
            "localizaci_n": "Colombia, Santander, Barbosa",
        },
    ),
    # Un proceso con dos modalidades: viola la dependencia proceso -> modalidad.
    (
        "proceso con modalidad A",
        {"proceso_de_compra": "PROC-DUPLICADO", "modalidad_de_contratacion": "Mínima cuantía"},
    ),
    (
        "mismo proceso con modalidad B",
        {
            "proceso_de_compra": "PROC-DUPLICADO",
            "modalidad_de_contratacion": "Licitación pública",
            "ultima_actualizacion": "2030-01-01T00:00:00.000",
        },
    ),
]


def generar_filas(n: int = 60, semilla: int = 42) -> list[dict[str, str]]:
    """Devuelve ``n`` filas (mínimo, la cantidad de casos raros) con las 95 columnas."""
    rng = random.Random(semilla)
    n = max(n, len(CASOS_RAROS))
    filas = [_fila_base(i, rng) for i in range(n)]
    for i, (_, cambios) in enumerate(CASOS_RAROS):
        filas[i].update(cambios)
    return filas


def escribir_csv(ruta: str | Path, filas: list[dict[str, str]]) -> Path:
    """Escribe las filas en un CSV con encabezado (mismo formato que la API)."""
    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("w", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(f, fieldnames=COLUMNAS_CRUDAS)
        escritor.writeheader()
        escritor.writerows(filas)
    return ruta
