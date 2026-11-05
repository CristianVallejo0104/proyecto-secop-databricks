# Atajos para no memorizar comandos. Uso: make test, make pipeline, make mlflow...
PY ?= python3
IMAGEN ?= secop-validador:0.1

.PHONY: instalar test lint compilar validar notebooks-prueba pipeline mlflow docker

instalar:            ## Instala dependencias con versiones fijas
	$(PY) -m pip install -r requirements-dev.txt

test:                ## Ejecuta las pruebas (necesita Java 17+ para PySpark)
	$(PY) -m pytest

lint:                ## Revisa estilo y errores con ruff (incluye los notebooks)
	$(PY) -m ruff check .

compilar:            ## Comprueba que todos los .py tengan sintaxis válida
	$(PY) -m py_compile $$(git ls-files '*.py')

validar:             ## Valida el esquema con datos sintéticos (sin Spark)
	$(PY) scripts/validar_esquema.py

notebooks-prueba:    ## Ejecuta los notebooks 02-06 con datos SINTÉTICOS (no toca datos/)
	$(PY) scripts/ejecutar_notebooks.py --sinteticos

pipeline:            ## Ejecuta los notebooks 01-06 con datos REALES (descarga lo que falte)
	$(PY) scripts/ejecutar_notebooks.py

mlflow:              ## Abre la interfaz de MLflow (puerto 5000) para ver los experimentos
	$(PY) -m mlflow ui --backend-store-uri datos/mlruns --port 5000

docker:              ## Construye la imagen y ejecuta la validación dentro
	docker build -t $(IMAGEN) .
	docker run --rm $(IMAGEN)
