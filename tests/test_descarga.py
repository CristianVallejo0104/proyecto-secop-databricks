"""Pruebas de la descarga SIN internet: la API se reemplaza por respuestas falsas."""

import json

import pytest

from secop import descarga
from secop.esquema import COLUMNAS_PERSONALES


class RespuestaFalsa:
    def __init__(self, status_code=200, contenido=b"", datos=None):
        self.status_code = status_code
        self.content = contenido
        self._datos = datos

    def json(self):
        return self._datos


class ApiFalsa:
    """Imita la API: responde conteos y páginas CSV, y registra cada petición."""

    def __init__(self, filas_por_mes=5, fallas_iniciales=0):
        self.filas, self.fallas, self.peticiones = filas_por_mes, fallas_iniciales, []

    def __call__(self, url, params):
        self.peticiones.append((url, dict(params)))
        if self.fallas > 0:
            self.fallas -= 1
            return RespuestaFalsa(status_code=503)
        if url.endswith(".json"):
            return RespuestaFalsa(datos=[{"n": str(self.filas)}])
        return RespuestaFalsa(contenido=f"id\n{params['$offset']}\n".encode())


@pytest.fixture
def datos_temporales(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOP_DATOS", str(tmp_path))
    return tmp_path


def test_filtro_mes_diciembre_pasa_al_anio_siguiente():
    texto = descarga.filtro_mes(2019, 12)
    assert "'2019-12-01T00:00:00'" in texto and "'2020-01-01T00:00:00'" in texto


def test_reintenta_ante_503_y_espera_cada_vez_mas():
    api, esperas = ApiFalsa(fallas_iniciales=2), []
    descarga.pedir_con_reintentos("u.json", {}, obtener=api, dormir=esperas.append, espera=1)
    assert esperas == [1, 2]  # espera exponencial: 1, 2, (4...)


def test_no_reintenta_un_400():
    def obtener(url, params):
        return RespuestaFalsa(status_code=400)

    with pytest.raises(RuntimeError, match="HTTP 400"):
        descarga.pedir_con_reintentos("u", {}, obtener=obtener, dormir=lambda s: None)


def test_se_rinde_despues_de_n_intentos():
    api = ApiFalsa(fallas_iniciales=99)
    with pytest.raises(RuntimeError, match="3 intentos"):
        descarga.pedir_con_reintentos("u", {}, intentos=3, obtener=api, dormir=lambda s: None)


def test_descarga_por_paginas_y_marca_el_mes(datos_temporales):
    api = ApiFalsa(filas_por_mes=5)
    assert descarga.descargar_mes(2019, 3, obtener=api, tamano_pagina=2) == 3  # 5 filas / 2
    carpeta = datos_temporales / "raw" / "contratos" / "anio=2019"
    assert sorted(p.name for p in carpeta.iterdir()) == [
        "mes_03.listo",
        "mes_03_parte_000.csv",
        "mes_03_parte_001.csv",
        "mes_03_parte_002.csv",
    ]


def test_reanuda_sin_repetir_paginas(datos_temporales):
    carpeta = datos_temporales / "raw" / "contratos" / "anio=2019"
    carpeta.mkdir(parents=True)
    (carpeta / "mes_03_parte_000.csv").write_text("id\n0\n")  # ya descargada antes
    api = ApiFalsa(filas_por_mes=5)
    assert descarga.descargar_mes(2019, 3, obtener=api, tamano_pagina=2) == 2
    # Un mes marcado como listo no vuelve a pedir nada
    assert descarga.descargar_mes(2019, 3, obtener=api, tamano_pagina=2) == 0


def test_nunca_pide_columnas_personales(datos_temporales):
    api = ApiFalsa(filas_por_mes=1)
    descarga.descargar_mes(2019, 1, obtener=api)
    paginas = [p for u, p in api.peticiones if u.endswith(".csv")]
    columnas = paginas[0]["$select"].split(",")
    assert not set(columnas) & set(COLUMNAS_PERSONALES)
    assert json.dumps(paginas)  # parámetros serializables
