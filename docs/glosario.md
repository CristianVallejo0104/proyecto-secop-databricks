# Glosario

**Acción (Spark):** operación que obliga a Spark a ejecutar el plan y devolver o escribir
un resultado: `count()`, `first()`, `display()`, `saveAsTable()`.

**Arquitectura medallón (bronce / plata / oro):** organización de los datos en capas de
calidad creciente. Bronce = copia fiel en texto; plata = datos limpios en tercera forma normal;
oro = tabla lista para análisis y modelo.

**AUC-PR:** área bajo la curva precisión-sensibilidad. Útil con clases desbalanceadas;
la referencia del azar es la tasa de positivos (~11 % en 2019), no 0,5.

**AUC-ROC:** área bajo la curva ROC. Probabilidad de que el modelo asigne mayor puntaje a
un contrato prorrogado que a uno no prorrogado. 0,5 = azar; 1 = perfecto.

**Broadcast join:** join en el que la tabla pequeña se copia a cada ejecutor para evitar
el shuffle de la grande. Se usa con `dim_entidad` y `dim_categoria` en `oro.py`.

**Codespaces:** entorno de desarrollo en la nube de GitHub, configurado con
`.devcontainer/devcontainer.json`.

**Commit:** fotografía de los cambios en Git, con un mensaje que explica qué cambió y por qué.

**Dependencia funcional (X → Y):** cada valor de X tiene un único valor de Y. Ej.: proceso
de compra → modalidad. Es la base para decidir en qué tabla va cada atributo.

**Dependencia transitiva:** llave → A → B, donde A no es llave. Ej.: contrato → proceso →
modalidad. La 3FN las elimina moviendo B a la tabla de A (`dim_proceso`).

**Delta Lake:** formato de tabla sobre Parquet con registro de transacciones: escrituras
atómicas, esquema validado e historial de versiones.

**Desnormalizar:** unir tablas normalizadas en una sola, a propósito, para analizar más
rápido. Es lo que hace la capa oro.

**Desbalance de clases:** cuando una clase es mucho menos frecuente que la otra (aquí ~11 %
de prórrogas). Se trata con pesos de clase y métricas adecuadas.

**Devcontainer:** archivo que describe un contenedor de desarrollo (imagen, Java,
extensiones); lo usan Codespaces y VS Code.

**Docker / imagen / contenedor:** Docker empaqueta software con sus dependencias. La
*imagen* es la plantilla (definida en el `Dockerfile`); el *contenedor* es una ejecución
de esa imagen.

**Driver / ejecutor:** en Spark, el *driver* coordina el trabajo y los *ejecutores*
procesan las particiones de datos en paralelo.

**F1:** media armónica de precisión y sensibilidad con un umbral dado. Alta solo si ambas
son altas.

**Fuga de información:** usar en el modelo información que no existe al momento de
predecir (estado, pagos, saldos, días añadidos). Infla las métricas de forma engañosa.

**Formas normales (1FN, 2FN, 3FN):** reglas de diseño de tablas. 1FN: valores atómicos y
sin grupos repetidos. 2FN: con llave compuesta, todo depende de la llave completa. 3FN: sin
dependencias transitivas. La capa plata está en 3FN.

**GBT (Gradient Boosted Trees):** árboles potenciados; cada árbol nuevo corrige los
errores de los anteriores. Es nuestro modelo de árboles en MLlib.

**GitHub Actions / CI:** integración continua; ejecuta automáticamente compilación, ruff,
validación y pytest en cada push y PR (`.github/workflows/ci.yml`).

**Idempotente:** que ejecutarlo varias veces da el mismo resultado. Ej.: recargar un año en
bronce con `replaceWhere` no duplica filas.

**Integridad referencial:** toda llave foránea debe existir en su tabla padre (p. ej.,
cada `codigo_entidad` de `contrato` existe en `dim_entidad`).

**Llave compuesta:** llave primaria de varias columnas. Ej.: (departamento, municipio) en
`dim_municipio`, porque hay municipios con el mismo nombre en distintos departamentos.

**Llave foránea (FK):** columna que apunta a la llave primaria de otra tabla. Ej.:
`contrato.codigo_entidad` → `dim_entidad.codigo_entidad`.

**Llave primaria (PK):** columna o columnas que identifican cada fila; únicas y no nulas.

**Lazy evaluation (evaluación perezosa):** Spark acumula transformaciones en un plan y solo
lo ejecuta ante una acción, lo que le permite optimizarlo.

**left_anti:** tipo de join que devuelve las filas de la izquierda sin pareja a la
derecha. Lo usamos para contar huérfanas (debe dar 0).

**MLflow (experimento, run):** herramienta para registrar modelos. Un *experimento* agrupa
*runs*; cada *run* guarda parámetros, métricas, etiquetas y, opcionalmente, el modelo.

**Modo ANSI:** modo estricto de SQL en Spark (activado en nuestra sesión): un `cast` inválido
produce error en lugar de NULL.

**Modo local (Spark):** Spark corriendo en una sola máquina: el driver y los ejecutores
son hilos de un mismo proceso y cada núcleo procesa una partición. Es como corremos todo
en el Codespace (`local[*]` = usar todos los núcleos).

**Normalizar:** organizar los datos en varias tablas relacionadas para que cada dato
esté en un solo lugar, sin redundancia.

**Notebook (Jupyter, `.ipynb`):** documento con celdas de código y de texto que se
ejecutan una por una. Los nuestros se suben a GitHub sin salidas.

**Partición (Delta):** división física de una tabla por los valores de una columna; aquí
bronce se particiona por `anio_carga`.

**Partición temporal:** separación entrenamiento/validación/prueba por fecha de firma, para
evaluar con contratos "futuros".

**PR (pull request):** solicitud para integrar una rama en otra (p. ej., en `main`), con
revisión y CI.

**Precisión:** de los contratos que el modelo marca como prorrogados, qué proporción
realmente lo fue.

**Regresión logística:** modelo lineal que estima la probabilidad de la clase 1. Es
nuestra línea base, simple e interpretable.

**replaceWhere:** opción de escritura de Delta que sobrescribe solo las filas que cumplen
una condición (p. ej., `anio_carga = 2019`).

**Sensibilidad (recall):** de los contratos realmente prorrogados, qué proporción detecta
el modelo.

**Shuffle:** redistribución de datos entre ejecutores (en joins y agregaciones). Es la
operación más costosa en Spark.

**Socrata / SODA:** plataforma y API de datos abiertos de datos.gov.co. Parámetros como
`$select`, `$where`, `$order`, `$limit` y `$offset`.

**Spark / PySpark:** motor de procesamiento distribuido de datos; PySpark es su interfaz
en Python.

**Time travel:** consultar una versión anterior de una tabla Delta (`VERSION AS OF n`),
gracias a su historial (`DESCRIBE HISTORY`).

**Transformación (Spark):** operación que define un DataFrame nuevo sin ejecutar nada:
`select`, `filter`, `join`, `withColumn`, `groupBy().agg()`.

**try_cast:** conversión de tipo que devuelve NULL si el valor no se puede convertir, en
lugar de fallar.

**UNSPSC:** clasificador de bienes y servicios de Naciones Unidas. Código de 8 dígitos:
segmento, familia, clase y producto (p. ej., "V1.80111600" → familia 8011, segmento 80).



**Winsorización:** recortar valores extremos a un tope. El valor del contrato se recorta en
2,2e9 (el p99 de 2019 es ≈ 2,17e9) antes de aplicar logaritmo.
