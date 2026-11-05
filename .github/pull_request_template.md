## Qué cambia
<!-- Una o dos frases. -->

## Por qué

## Cómo se probó
- [ ] `make test` pasa
- [ ] `make lint` pasa
- [ ] Notebook(s) ejecutados en Databricks (indicar cuáles):

## Lista de control
- [ ] Hice `git pull` antes de `git push` (regla de oro)
- [ ] No hay datos, salidas ni credenciales en el commit
- [ ] No se agregan columnas con datos personales (ver `src/secop/esquema.py`)
- [ ] No se usan variables posteriores a la firma en el modelo
- [ ] No se usa `cache()`, `persist()`, RDD, `sparkContext` ni `toPandas()` sobre tablas grandes
- [ ] Los resultados nuevos están en `docs/informe.md` (sin cifras inventadas)
