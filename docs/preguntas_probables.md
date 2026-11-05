# Preguntas probables y respuestas

Respuestas cortas para la sustentación. Cuando sirve, se muestra el código del repo.

---

## Spark

### 1. ¿Qué es la evaluación perezosa (lazy evaluation)?
Spark no ejecuta las transformaciones cuando se escriben: las anota en un **plan**.
Solo calcula cuando aparece una **acción** (contar, escribir, mostrar). Así puede
optimizar el plan completo, por ejemplo, leer solo las columnas necesarias o aplicar
los filtros antes de los joins.

```python
bronce = spark.table(config.TABLA_BRONCE)  # no lee nada todavía
contrato = plata.construir_contrato(bronce)  # sigue sin leer
contrato.write.format("delta").saveAsTable(...)  # aquí se ejecuta todo
```

### 2. ¿Diferencia entre transformación y acción? Ejemplos del proyecto.
- **Transformación:** devuelve un DataFrame nuevo y no ejecuta nada: `select`, `filter`,
  `withColumn`, `join`, `groupBy().agg()`. Ejemplo: `construir_oro` en `src/secop/oro.py`
  es solo una cadena de transformaciones.
- **Acción:** dispara el cálculo y devuelve un resultado o escribe: `count()`, `first()`,
  `collect()`, `display()`, `write...saveAsTable()`. Ejemplos: el `count()` que compara
  filas contra el servidor (notebook 01) y el `count()` de `filas_huerfanas` (notebook 03).

### 3. ¿Qué significa que los DataFrames son inmutables?
Un DataFrame nunca se modifica: cada transformación crea uno nuevo. Por eso escribimos
`df = df.withColumn(...)` y no "cambiamos" `df`. Ventaja: las funciones de `src/secop`
son puras, se pueden probar por separado y no tienen efectos ocultos.

### 4. ¿Qué es un broadcast join y dónde lo usan?
En un join normal Spark redistribuye (*shuffle*) las dos tablas por la llave. Si una
tabla es pequeña, es más barato enviar una copia completa a cada ejecutor. En
`src/secop/oro.py` hacemos broadcast de `dim_entidad` y `dim_categoria`, que son
pequeñas, para no mover la tabla grande de contratos.

```python
entidad = F.broadcast(dim_entidad.select("codigo_entidad", ...))
```

### 5. ¿Por qué evitan `toPandas()`?
`toPandas()` trae **todos** los datos a la memoria del driver (8 GB en Free Edition):
con cientos de miles de filas puede fallar y además pierde el procesamiento distribuido.
Por eso los descriptivos se agregan en Spark y solo se muestra el resultado pequeño.
El único uso está en el plan B de `06_modelo`: una **muestra** acotada por el widget
`max_filas_muestra` y con unas 30 columnas, para entrenar scikit-learn si MLlib falla.

### 6. ¿Por qué no usan `cache()` ni `persist()`?
En cómputo serverless de Databricks no están disponibles (tampoco RDD ni `sparkContext`).
Cada tabla se recalcula desde la anterior; con este volumen es rápido. Si algo fuera
lento, la alternativa es materializar el paso intermedio como tabla Delta.

### 7. ¿Por qué `try_cast` y no `cast`?
Serverless trabaja en **modo ANSI**: un `cast` de un texto inválido (por ejemplo
"No definido" a número) detiene todo el trabajo. `try_cast` devuelve NULL en ese caso,
y luego decidimos qué hacer con los nulos.

```python
def a_numero(c):
    return F.trim(_col(c)).try_cast("double")
```

---

## Datos y almacenamiento

### 8. ¿Qué es Delta Lake y qué es time travel?
Delta es un formato de tabla sobre Parquet con un registro de transacciones
(`_delta_log`). Da escrituras atómicas (o todo o nada), validación de esquema e
historial de versiones. Con ese historial se puede consultar el pasado:

```sql
DESCRIBE HISTORY workspace.secop.bronze_contratos;
SELECT * FROM workspace.secop.bronze_contratos VERSION AS OF 3;
```

### 9. ¿Qué hace `replaceWhere`?
Sobrescribe solo las filas que cumplen la condición. En el notebook 02 recargar 2019
reemplaza solo `anio_carga = 2019` y deja intactos los demás años. Así el notebook es
**idempotente**: repetirlo no duplica datos.

```python
escritor.option("replaceWhere", f"anio_carga = {anio}")
```

### 10. ¿Por qué normalizar en plata? ¿En qué forma normal está?
Plata está en **tercera forma normal (3FN)**. Cada dato vive en un solo lugar (el nombre
de una entidad no se repite en miles de filas), no hay redundancia que pueda
contradecirse y la integridad referencial se verifica con `left_anti`. Además separamos
`contrato` (lo que se sabe al firmar) de `contrato_ejecucion` (lo posterior): usar una
variable con fuga exigiría un join explícito, difícil de hacer por error.

### 10a. Expliquen 1FN, 2FN y 3FN con ejemplos de su base.
- **1FN (valores atómicos, sin grupos repetidos):** "Colombia, Antioquia, Medellín" se
  separó en departamento y municipio; "6 Mes(es)" en número (6) y unidad (mes); las 6
  columnas de fuentes de recursos pasaron a filas en `contrato_fuente_recurso`.
- **2FN (con llave compuesta, todo depende de la llave completa):** en
  `contrato_fuente_recurso` (contrato, fuente), el valor depende de las dos. En
  `dim_municipio` (departamento, municipio) no guardamos el país, porque depende solo
  del departamento: eso violaría 2FN.
- **3FN (sin dependencias transitivas):** la modalidad depende del proceso de compra, no
  del contrato, y por eso está en `dim_proceso`. El segmento UNSPSC depende de la familia
  (`dim_familia_unspsc`), no del código de categoría.

### 10b. ¿Cómo saben que esas dependencias se cumplen?
No lo suponemos: lo medimos. `plata.revisar_dependencias` cuenta, para cada dependencia
X → Y, cuántos valores de X aparecen con más de un Y en los datos. Si da 0, se cumple.
Si no da 0 (en 2019 hay 8 entidades con dos nombres), la dimensión aplica una regla
explícita y lo reportamos.

```python
display(plata.revisar_dependencias(bronce))  # notebook 03, sección 2
```

### 10c. ¿Por qué la llave de municipio es compuesta?
Porque el nombre del municipio no es único: Barbosa existe en Antioquia y en Santander.
La llave es (departamento, municipio). Hay un test con ese caso exacto.

### 10d. Si plata está normalizada, ¿por qué oro no?
Oro se desnormaliza a propósito para analizar: una fila por contrato con todo unido,
sin repetir 6 joins en cada consulta. Normalizar para almacenar, desnormalizar para
analizar. Oro no tiene información nueva: se reconstruye desde plata cuando se quiera.

### 10e. ¿Por qué `es_pyme` está en contrato y no en proveedor?
Porque cambia con el tiempo: una empresa puede ser pyme en 2019 y no en 2020. El valor
depende del contrato (del momento de la firma), así que no hay dependencia funcional
proveedor → es_pyme. `es_grupo` (consorcio o unión temporal) sí es estable y está en
`dim_proveedor`.

### 11. ¿Por qué eliminan los datos personales desde bronce y usan `$select`?
Lo más seguro es no tener el dato. Para descargas nuevas pedimos a la API solo las
columnas permitidas con `$select`. Si un CSV antiguo las trae, `preparar_bronce` las
descarta antes de escribir la primera tabla. Las columnas están en la lista
`COLUMNAS_PERSONALES` de `src/secop/esquema.py`, y un test verifica que esos nombres no
aparezcan en ningún otro archivo del repositorio.

### 12. ¿Qué pasa si el profesor no aprueba esta base de datos?
Los nombres de columnas crudas están solo en `src/secop/esquema.py`, y las rutas, tablas
y parámetros en `src/secop/config.py`. Cambiar de base es ajustar esos dos archivos y
las funciones de limpieza que correspondan; los notebooks y las pruebas siguen igual.

### 13. ¿Por qué no usan 2025 y 2026?
Su etiqueta está incompleta: esos contratos todavía pueden recibir prórrogas. Si los
usáramos, muchos aparecerían como "sin prórroga" solo porque aún no ha pasado el tiempo.
Quedan en oro con `etiqueta_completa = false` y se excluyen del entrenamiento.

---

## Modelo

### 14. ¿Cómo manejan el desbalance de clases?
Con ~11 % de positivos usamos pesos "balanceados": `peso_k = n / (2 · n_k)`. Así un
positivo pesa unas 4,5 veces y un negativo poco más de la mitad, y ambas clases aportan
lo mismo a la pérdida. No usamos exactitud: decir siempre "no" daría ~89 % y sería
inútil. Usamos AUC-ROC, AUC-PR y F1. Ojo: el AUC-PR de un modelo al azar es la tasa de
positivos, no 0,5; esa es la referencia con la que hay que comparar.

### 15. ¿Qué es la fuga de información y cómo la evitaron?
Es usar en el modelo algo que no se conoce al momento de predecir. Excluimos estado del
contrato, valores de ejecución (facturado, pagado, saldos), fechas de liquidación y
`dias_adicionados` (solo se usa para la etiqueta). Además, no sabemos si la fecha de fin
ya incluye la prórroga, así que entrenamos tres variantes (sin plazo, con duración y con
plazo). Si el AUC sube más de 0,05 al añadir el plazo, lo tratamos como sospechoso.

### 16. ¿Por qué una partición temporal y no aleatoria?
El modelo se usaría con contratos futuros. Si mezclamos meses al azar, el modelo "ve"
patrones del futuro y las métricas salen optimistas. Entrenamos con fechas de firma
anteriores, elegimos el umbral en validación y medimos una sola vez en prueba.

### 17. ¿Qué es MLflow y qué registran?
Es una herramienta para registrar experimentos. Por cada combinación (variante ×
algoritmo) guardamos parámetros (cortes, semilla, pesos, variables), métricas (AUC-ROC,
AUC-PR, F1, precisión, sensibilidad, exactitud, umbral) y etiquetas de reproducibilidad
(versión de `secop`, commit de git, versión de Spark, motor usado).

### 18. ¿Y si MLlib no funciona en serverless?
El notebook 06 prueba MLlib con una muestra pequeña. Si falla, cambia automáticamente al
plan B: la preparación sigue en Spark y scikit-learn entrena sobre una muestra acotada.
El motivo del cambio queda registrado en MLflow.

---

## Herramientas y reproducibilidad

### 19. ¿Qué aportan Docker, Codespaces y GitHub Actions?
- **Docker:** una imagen fija que valida el esquema con datos sintéticos; funciona igual
  en cualquier computador.
- **Codespaces:** el mismo entorno de desarrollo (Python, Java, dependencias) para los dos,
  en el navegador.
- **GitHub Actions:** en cada push y PR compila, revisa estilo, valida el esquema, ejecuta
  pytest y construye la imagen. Si algo se rompe, se ve antes de llegar a `main`.

### 20. ¿Cómo aseguran la reproducibilidad?
Versiones exactas con `==` en los `requirements`, semilla fija (`SEMILLA = 42`), datos
sintéticos con semilla, versión del código en MLflow y etiquetas de git (`v0.1`, ...).

### 21. ¿Qué es un Git folder de Databricks?
Es una carpeta del workspace enlazada a un repositorio de GitHub. Desde ahí se hace pull,
commit y push. Los notebooks importan `src/secop` añadiendo `src` a `sys.path`.

### 22. ¿Qué es la "regla de oro"?
Hacer `git pull` antes de `git push`. Así se integran primero los cambios del compañero y
se evitan rechazos o, peor, sobrescribir su trabajo. Nunca usamos `push --force` en ramas
compartidas.

```bash
git pull --rebase origin trabajo/proyecto-completo
```
```bash
git push origin trabajo/proyecto-completo
```
