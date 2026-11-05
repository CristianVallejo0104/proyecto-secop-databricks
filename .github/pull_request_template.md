## Qué cambia
<!-- Una o dos frases. -->

## Por qué

## Cómo se probó
- [ ] `make test` pasa
- [ ] `make lint` pasa
- [ ] `make notebooks-prueba` pasa
- [ ] Notebook(s) ejecutados con datos reales en el Codespace (indicar cuáles):

## Lista de control
- [ ] Hice `git pull` antes de `git push` (regla de oro)
- [ ] No hay datos, salidas ni credenciales en el commit
- [ ] No se agregan columnas con datos personales (ver `src/secop/esquema.py`)
- [ ] No se usan variables posteriores a la firma en el modelo
- [ ] Los notebooks se suben SIN salidas (el test lo verifica)
- [ ] No se usa `toPandas()` sobre tablas grandes (solo sobre resultados agregados o muestras)
- [ ] Los resultados nuevos están en `docs/informe.md` (sin cifras inventadas)
