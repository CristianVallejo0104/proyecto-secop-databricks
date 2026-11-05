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

Delta es Parquet más un registro de transacciones (`_delta_log`). Es un formato
abierto: funciona igual en un computador (con la librería `delta-spark`) que en
plataformas como Databricks. Cada tabla es una carpeta en `datos/delta/<tabla>/`.

## 2. Arquitectura bronce → plata → oro

- **Bronce:** copia fiel de la fuente, todo como texto. Si una regla de limpieza
  estaba mal, se corrige y se reprocesa desde bronce sin volver a descargar
  (la descarga toma horas por la lentitud de la API).
- **Plata:** datos limpios en tercera forma normal (ver §3).
- **Oro:** una tabla por caso de uso (aquí, el modelo).

Alternativa descartada: ir de CSV directo a la tabla del modelo. Más corto, pero
cada error obligaría a descargar de nuevo y no habría tablas reutilizables.

## 3. Plata en tercera forma normal (3FN)

### 3.1 Por qué normalizar

- **Un dato, un lugar:** el nombre de una entidad está una vez en `dim_entidad`, no
  repetido en cada contrato. Si cambia, se corrige en una fila y no en miles.
- **Sin contradicciones:** en 2019, 8 códigos de entidad tenían dos nombres (completo
  y sigla). En una tabla normalizada ese conflicto se resuelve una sola vez, con una
  regla explícita (el nombre más largo).
- **Separar "al firmar" de "después":** `contrato` solo tiene lo que se sabe al
  firmar; `contrato_ejecucion` tiene estado, pagos, saldos y días añadidos.
- **Integridad verificable:** con `left_anti` se comprueba que toda llave foránea
  existe en su tabla padre (debe dar 0 en las 10 relaciones).

### 3.2 Base teórica: dependencia funcional

"X → Y" (X determina a Y) significa que cada valor de X tiene **un solo** valor de Y.
Ejemplo: `proceso_de_compra → modalidad` dice que un proceso tiene una sola modalidad,
aunque genere varios contratos. Las formas normales se definen con estas dependencias:

| Forma | Regla | Pregunta para revisarla |
|---|---|---|
| **1FN** | Cada celda tiene un valor atómico y no hay grupos repetidos | ¿Alguna celda guarda varios datos o hay columnas "dato1, dato2, dato3"? |
| **2FN** | 1FN + ningún atributo depende solo de **una parte** de una llave compuesta | En las tablas con llave de 2 columnas, ¿algo depende de una sola? |
| **3FN** | 2FN + ningún atributo que no es llave depende de **otro atributo que no es llave** | ¿Hay cadenas llave → A → B? Entonces B va en otra tabla |

### 3.3 Cómo se aplicó cada forma normal

**1FN: valores atómicos y sin grupos repetidos**

| Antes (bronce) | Problema | Después (plata) |
|---|---|---|
| `localizaci_n` = "Colombia, Antioquia, Medellín" | Tres datos en una celda | `departamento`, `municipio` y `pais`, cada uno en su tabla |
| `duraci_n_del_contrato` = "6 Mes(es)" | Número y unidad juntos | `duracion_numero` = 6, `duracion_unidad` = "mes" |
| 6 columnas de fuentes (PGN, SGP, regalías...) | Grupo repetido | `contrato_fuente_recurso`: una fila por (contrato, fuente) |

**2FN: dependencia de la llave completa**

Solo aplica a tablas con llave compuesta (las de llave simple cumplen 2FN automáticamente):
- `contrato_fuente_recurso` (`id_contrato`, `fuente`): `valor` es el aporte de *esa*
  fuente a *ese* contrato, así que depende de las dos partes. ✔
- `dim_municipio` (`departamento`, `municipio`): la llave es compuesta porque hay
  municipios con el mismo nombre en departamentos distintos (por ejemplo, Barbosa en
  Antioquia y en Santander). El país depende **solo** del departamento; si estuviera
  aquí violaría 2FN, por eso está en `dim_departamento`. ✔

**3FN: sin dependencias transitivas**

| Dependencia transitiva que había | Solución |
|---|---|
| contrato → proceso → modalidad, justificación | Tabla `dim_proceso` |
| contrato → categoría → familia → segmento | `dim_categoria` (código → familia) y `dim_familia_unspsc` (familia → segmento) |
| entidad → departamento → país | `dim_departamento`; la entidad guarda solo la FK (departamento, municipio) |
| contrato → proveedor → es grupo (consorcio o unión temporal) | `es_grupo` pasa a `dim_proveedor` |
| número + unidad → días de duración (dato calculado) | No se guarda en plata; se calcula en oro con `dias_desde_duracion` |
| fecha del primer contrato del proveedor (calculable desde `contrato`) | Eliminada |

### 3.4 Casos que podrían parecer violaciones y por qué no lo son

- **`es_pyme` se queda en `contrato`:** una empresa puede ser pyme en 2019 y dejar de
  serlo en 2020. El valor no depende solo del proveedor sino del momento del
  contrato, así que `codigo_proveedor → es_pyme` **no** es una dependencia funcional.
  Además, guardarlo en el contrato conserva el valor al firmar (evita fuga).
- **`n_nombres` (entidad) y `n_modalidades` (proceso):** son datos de calidad sobre la
  fuente (cuántos valores distintos traía). No se pueden recalcular desde plata,
  porque plata ya guarda un solo valor, así que no son redundantes.
- **`nit_entidad` en `dim_entidad`:** el NIT también identifica a la entidad (es una
  llave candidata). Que dos llaves se determinen mutuamente no viola 3FN.
- **`tipo_de_contrato` en `contrato`:** se asume que es del contrato. Si los datos
  muestran que siempre es igual dentro de un proceso, podría moverse a `dim_proceso`.

### 3.5 Las dependencias se verifican con los datos

Un diseño normalizado es tan bueno como las dependencias en que se basa. Por eso
`plata.revisar_dependencias` cuenta en bronce, para cada dependencia X → Y, cuántos
valores de X aparecen con más de un Y. El notebook 03 muestra la tabla:

| Resultado | Interpretación |
|---|---|
| 0 violaciones | La dependencia se cumple: la tabla está bien separada |
| > 0 violaciones | La fuente tiene datos inconsistentes. La dimensión aplica una regla explícita (nombre más largo, valor más reciente) y el número se reporta en el informe |

Referencia: en 2019, "entidad → nombre" tenía 8 violaciones. Las demás: PENDIENTE.

### 3.6 Por qué oro se desnormaliza

Plata se normaliza para **guardar** bien; oro se desnormaliza a propósito para
**analizar**. El modelo y los descriptivos necesitan una fila por contrato con todo,
y repetir 6 joins en cada consulta sería lento y propenso a errores. Es el patrón
estándar: normalizar para almacenar, desnormalizar para analizar. Oro no guarda
información nueva: se puede reconstruir en cualquier momento desde plata.

## 4. Datos personales: se eliminan antes de la primera tabla

- La lista está en un solo lugar: `COLUMNAS_PERSONALES` en `src/secop/esquema.py`
  (nombres, documentos, género, domicilio, nacionalidad y datos bancarios de
  representantes, supervisores y ordenadores, y nombre y documento del proveedor).
- **La descarga ya no las pide:** el notebook 01 usa `$select` con las columnas
  permitidas, así esos datos nunca llegan al disco.
- **Bronce las descarta** si vienen (descargas antiguas de 2019-2020 sí las traen).
  Por eso bronce tiene 75 columnas y no 95: la restricción de privacidad prevalece
  sobre "copiar todo".
- `dim_proveedor` solo guarda el **código interno de SECOP**, si es un **grupo**
  (consorcio o unión temporal) y el **tipo** de
  documento (persona natural o jurídica), que no identifican a nadie por sí solos.
- **Textos libres** (descripción, objeto, dirección de ejecución) se quedan en
  bronce, pero no pasan a plata ni a oro: pueden mencionar nombres de personas.
- Un test (`tests/test_esquema_y_privacidad.py`) falla si los nombres de columnas
  personales aparecen en cualquier archivo distinto de `esquema.py`, y otro
  (`tests/test_capas.py`) verifica que ninguna tabla de salida los contenga.
- Pendiente para el equipo: los CSV que se descargaron antes con las 95 columnas
  (2019-2020) contienen esas columnas. Hay que borrarlos donde estén y descargar
  de nuevo con el notebook 01 (ver `docs/runbook.md` §5).
- Los notebooks se suben a GitHub **sin salidas** (un test lo exige): así una
  tabla mostrada durante el análisis nunca termina en el repositorio.

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
| Textos en columnas numéricas | `try_cast`: NULL en vez de error (la sesión usa modo ANSI) | `a_numero` |
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
  cambian con los parámetros de 06 sin editar código.

## 9. Partición temporal, pesos y métricas

- **Temporal y no aleatoria:** el modelo se usaría con contratos futuros. Mezclar
  meses deja que el modelo "vea" patrones del futuro y da métricas optimistas.
- **Pesos de clase** `n / (2·n_k)`: con ~11 % de positivos, sin pesos el modelo
  tiende a predecir siempre "no".
- **Métricas:** AUC-ROC (capacidad de ordenar), AUC-PR (calidad en la clase rara;
  el azar da la tasa de positivos), F1, precisión y sensibilidad. La exactitud se
  reporta solo para mostrar que engaña.
- **Umbral** elegido con validación (máximo F1) y aplicado una sola vez en prueba.

## 10. Entorno: Codespaces con PySpark en modo local (y no Databricks)

El proyecto se pensó primero para Databricks Free Edition, pero el equipo decidió
ejecutarlo todo en Python dentro de **GitHub Codespaces**.

| Criterio | Codespaces + PySpark local | Databricks |
|---|---|---|
| Entorno igual para los dos | Sí: `devcontainer.json` fija Python, Java y librerías | Sí, pero aparte del repositorio |
| Mismo lugar que el código | Sí: se programa, prueba y ejecuta en el mismo sitio | No: hay que sincronizar con Git folders |
| Pruebas automáticas de los notebooks | Sí: la CI ejecuta los mismos `.ipynb` | No directamente |
| Spark, Delta, MLflow | Sí (modo local) | Sí (cómputo administrado) |
| Escala | Una máquina (4 núcleos, 16 GB en la opción grande) | Varias máquinas |

**Por qué alcanza:** 2019 y 2020 suman unos 500 mil contratos, que una máquina de
Codespaces procesa con Spark en modo local. El código es el mismo PySpark que
correría en un clúster: si la base creciera, solo habría que cambiar dónde se crea
la sesión (`entorno.crear_spark`).

**Qué es Spark en modo local:** el *driver* y los *ejecutores* son hilos dentro de
un mismo proceso. Spark igual divide los datos en particiones y las procesa en
paralelo, una por núcleo. Se ve en la interfaz de Spark (puerto 4040).

| Riesgo o límite | Decisión |
|---|---|
| Memoria (8-16 GB según la máquina) | Nada de `toPandas()` sobre tablas grandes: se agrega en Spark y solo se trae el resultado (`entorno.mostrar` usa `limit`) |
| GBT lento en pocos núcleos | Parámetro `ITER_GBT` y plan B con scikit-learn sobre una muestra acotada (máx. 200.000 filas por partición) |
| Modo ANSI activado | `try_cast` en todas las conversiones |
| Disco del Codespace | `datos/` solo guarda los años que se usan; las tablas Delta se regeneran desde los CSV |
| Cupo gratuito de Codespaces | Detener el Codespace al terminar (runbook §1) |
| `cache()` | No se usa: cada tabla se materializa en Delta y se lee de ahí; el código sigue siendo compatible con plataformas donde `cache()` no existe |

## 11. Código en `src/` y notebooks delgados

La lógica vive en funciones puras de `src/secop/` y los notebooks solo leen,
llaman y escriben. Ventajas: pruebas con pytest sin ejecutar el pipeline,
reutilización, y diffs legibles en los PR (los cambios de lógica están en `.py`;
los notebooks cambian poco y se suben sin salidas).

## 12. Cambiar de base de datos

El proyecto se diseñó para que el profesor pueda pedir otra base:
- `src/secop/esquema.py`: columnas, personales, posteriores a la firma.
- `src/secop/config.py`: URL de la API, rutas, tablas y variables del modelo.
- Las funciones de limpieza son genéricas (reciben nombres de columna).
- `plata.py` y `oro.py` serían lo único por adaptar a la nueva estructura: hay que
  identificar de nuevo las dependencias funcionales y repetir la normalización (§3).
