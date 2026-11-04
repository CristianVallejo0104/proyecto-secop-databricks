# Atajos para no memorizar comandos. Uso: make test, make lint, make docker.
PY ?= python3
IMAGEN ?= secop-validador:0.1

.PHONY: instalar test lint compilar validar docker

instalar:            ## Instala dependencias de desarrollo con versiones fijas
	$(PY) -m pip install -r requirements-dev.txt

test:                ## Ejecuta las pruebas (necesita Java 17+ para PySpark)
	$(PY) -m pytest

lint:                ## Revisa estilo y errores con ruff
	$(PY) -m ruff check .

compilar:            ## Comprueba que todos los .py tengan sintaxis válida
	$(PY) -m py_compile $$(git ls-files '*.py')

validar:             ## Valida el esquema con datos sintéticos (sin Spark)
	$(PY) scripts/validar_esquema.py

docker:              ## Construye la imagen y ejecuta la validación dentro
	docker build -t $(IMAGEN) .
	docker run --rm $(IMAGEN)
