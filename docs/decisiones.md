# Decisiones de diseño (y por qué)

Cada decisión indica la alternativa descartada. Sirve para responder "¿por qué
hicieron X y no Y?" en la sustentación.

## 1. Delta Lake en lugar de CSV o Parquet

| Necesidad | CSV/Parquet | Delta |
|---|---|---|
| Que una escritura fallida no deje la tabla a medias | No | Sí: transacciones atómicas (todo o nada) |
| Recargar un solo año sin tocar los demás | Manual (borrar carpetas) | `replaceWhere "anio_carga = 2019"` |
| Ver o recuperar una versión anterior | No | *Time travel*: `VERSION AS OF n`, `DESCRIBE HISTORY` |
| Rechazar datos con esquema distinto | No | Sí (validación de esquema) |

Delta es Parquet más un registro de transacciones (`_delta_log`). Es el formato por
defecto de Databricks y de Unity Catalog.

## 2. Arquitectura bronce → plata → oro

- **Bronce:** copia fiel de la fuente, todo como texto. Si una regla de limpieza
  estaba mal, se corrige y se reprocesa desde bronce sin volver a descargar
  (la descarga toma horas por la lentitud de la API).
- **Plata:** datos limpios y normalizados.
- **Oro:** una tabla por caso de uso (aquí, el modelo).

Alternativa descartada: ir de CSV directo a la tabla del modelo. Más corto, pero
cada error obligaría a descargar de nuevo y no habría tablas reutilizables.

## 3. Normalizar en plata

- **Un dato, un lugar:** el nombre de una entidad está una vez en `dim_entidad`,
  no repetido en cada contrato. En 2019, 8 códigos de entidad tenían dos nombres
  (nombre completo y sigla); en una tabla normalizada esa inconsistencia se
  resuelve en un solo sitio (regla: el nombre más largo).
- **Separar "al firmar" de "después":** `contrato` solo tiene lo que se sabe al
  firmar; `contrato_ejecucion` tiene estado, pagos, saldos y días añadidos. Para
  usar por error una variable posterior habría que unir explícitamente esa tabla.
- **Formato largo para fuentes de recursos:** las 6 columnas de fuentes se apilan
  con `stack` en `contrato_fuente_recurso (id_contrato, fuente, valor)`. Agregar
  una fuente nueva no cambia el esquema.
- **Integridad referencial verificable:** con `left_anti` se comprueba que toda
  llave foránea existe en su dimensión (debe dar 0).

## 4. Datos personales: se eliminan antes de la primera tabla

- La lista está en un solo lugar: `COLUMNAS_PERSONALES` en `src/secop/esquema.py`
  (nombres, documentos, género, domicilio, nacionalidad y datos bancarios de
  representantes, supervisores y ordenadores, y nombre y documento del proveedor).
- **La descarga ya no las pide:** el notebook 01 usa `$select` con las columnas
  permitidas, así esos datos nunca llegan al Volume.
- **Bronce las descarta** si vienen (descargas antiguas de 2019-2020 sí las traen).
  Por eso bronce tiene 75 columnas y no 95: la restricción de privacidad prevalece
  sobre "copiar todo".
- `dim_proveedor` solo guarda el **código interno de SECOP** y el **tipo** de
  documento (persona natural o jurídica), que no identifican a nadie por sí solos.
- **Textos libres** (descripción, objeto, dirección de ejecución) se quedan en
  bronce, pero no pasan a plata ni a oro: pueden mencionar nombres de personas.
- Un test (`tests/test_esquema_y_privacidad.py`) falla si los nombres de columnas
  personales aparecen en cualquier archivo distinto de `esquema.py`, y otro
  (`tests/test_capas.py`) verifica que ninguna tabla de salida los contenga.
- Pendiente para el equipo: los CSV antiguos del Volume (2019-2020) aún contienen
  esas columnas. Recomendación: volver a descargarlos con el notebook 01 y borrar
  los anteriores (ver runbook).

## 5. Variables excluidas por fuga de información

El modelo responde "¿habrá prórroga?" **en el momento de firmar**. Cualquier dato
que se conozca después no puede ser variable, aunque mejore las métricas.

| Excluido | Motivo |
|---|---|
| Estado del contrato | Se actualiza durante la ejecución ("Prorrogado", "Terminado"...) |
| Valores facturado, pagado, pendiente, amortizado; saldos | Ocurren después de firmar |
| Fechas de liquidación, notificación de prórroga, última actualización | Posteriores |
| Días adicionados | Es la etiqueta; usarla como variable sería hacer trampa |
| Fecha de fin y duración (plazo) | **Dudosa:** no sabemos si ya incluye los días añadidos |

La lista vive en `COLUMNAS_POSTERIORES_FIRMA` (`esquema.py`). Para el plazo se
hace un experimento: tres variantes (`sin_plazo`, `con_duracion`, `con_plazo`).
Si añadir el plazo sube el AUC-ROC en más de 0,05, se considera sospechoso y el
modelo final es `sin_plazo`. Resultado: PENDIENTE.

## 6. Etiqueta y filtros de oro

- `etiqueta = 1` si `dias_adicionados > 0`; `0` si es 0; **sin etiqueta** si está
  vacío (no se asume que vacío = 0).
- Se excluyen: fecha de firma vacía (no hay partición temporal posible) y valor
  del contrato <= 0 o no numérico (error de registro).
- Observación por verificar: en 2019 hay 142.090 filas, 833 con valor <= 0, y el
  oro previo tenía 141.257 filas (142.090 − 833). Coincide con el filtro de valor;
  se confirma al ejecutar.

## 7. Limpieza

| Problema observado (2019) | Regla | Función |
|---|---|---|
| Textos en columnas numéricas | `try_cast`: NULL en vez de error (serverless usa modo ANSI) | `a_numero` |
| Fechas "2019-03-15T00:00:00.000" | Primeros 10 caracteres → fecha | `a_fecha` |
| Estado con mayúsculas mezcladas | `initcap` | `limpiar_estado` |
| "No Definido" en muchas variantes | Una sola categoría "No Definido" | `limpiar_categoria` |
| Valores <= 0 y máximo de 2,8e16 | <= 0 → NULL; recorte en 2,2e9 (p99 ≈ 2,17e9); `log1p` | `valor_recortado`, `log_valor` |
| Localización "País, Depto, Ciudad" | Separar en 3 columnas | `separar_localizacion` |
| Duración "6 Mes(es)", "No definido" | Número + unidad + días aproximados | `separar_duracion` |
| Código "V1.80111600" | Familia UNSPSC `(\d{4})\d{4}$` → "8011"; segmento "80" | `familia_unspsc` |
| ~48 % con inicio antes de la firma | No se corrige: se convierte en variable | `inicio_antes_de_firma` |
| Origen de recursos casi constante | No se usa en el modelo (no aporta información) | — |

## 8. Años de entrenamiento

- **2025 y 2026 nunca se usan para entrenar ni evaluar:** sus contratos aún pueden
  recibir prórrogas, así que muchos "0" de hoy serán "1" mañana (etiqueta incompleta).
  En oro quedan marcados con `etiqueta_completa = false`.
- Datos disponibles hoy: 2019 y 2020. Partición por defecto:
  entrenamiento hasta marzo de 2020, validación abril-junio 2020, prueba julio-diciembre 2020.
- Riesgo conocido: 2020 es el año de la pandemia; el comportamiento de las prórrogas
  pudo cambiar. Cargar más años (2021-2024) reduce ese riesgo; los cortes se
  cambian con widgets sin editar código.

## 9. Partición temporal, pesos y métricas

- **Temporal y no aleatoria:** el modelo se usaría con contratos futuros. Mezclar
  meses deja que el modelo "vea" patrones del futuro y da métricas optimistas.
- **Pesos de clase** `n / (2·n_k)`: con ~11 % de positivos, sin pesos el modelo
  tiende a predecir siempre "no".
- **Métricas:** AUC-ROC (capacidad de ordenar), AUC-PR (calidad en la clase rara;
  el azar da la tasa de positivos), F1, precisión y sensibilidad. La exactitud se
  reporta solo para mostrar que engaña.
- **Umbral** elegido con validación (máximo F1) y aplicado una sola vez en prueba.

## 10. Entorno: Databricks Free Edition (serverless)

| Restricción | Decisión |
|---|---|
| Sin `cache()`/`persist()` | Recalcular desde Delta o materializar pasos intermedios como tablas |
| Sin RDD ni `sparkContext` | Solo API de DataFrames |
| 8 GB de memoria | Nada de `toPandas()` sobre tablas grandes; agregar en Spark y mostrar el resultado |
| MLlib podría fallar | Plan B: scikit-learn sobre una muestra acotada (máx. 200.000 filas por partición), con la preparación en Spark |
| Modo ANSI | `try_cast` en todas las conversiones |

## 11. Código en `src/` y notebooks delgados

La lógica vive en funciones puras de `src/secop/` y los notebooks solo leen,
llaman y escriben. Ventajas: pruebas con pytest sin Databricks, revisión de código
en PR legible (archivos `.py`, no JSON de notebooks) y reutilización.

## 12. Cambiar de base de datos

El proyecto se diseñó para que el profesor pueda pedir otra base:
- `src/secop/esquema.py`: columnas, personales, posteriores a la firma.
- `src/secop/config.py`: URL de la API, rutas, tablas y variables del modelo.
- Las funciones de limpieza son genéricas (reciben nombres de columna).
- `plata.py` y `oro.py` serían lo único por adaptar a la nueva estructura.
