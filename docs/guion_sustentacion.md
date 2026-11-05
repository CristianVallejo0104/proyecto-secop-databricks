# Guion de sustentación (5 minutos, dos voces)

Voces: **C** = Cristian, **JP** = JP. Los marcadores `[PENDIENTE: ...]` se llenan
con los resultados reales después de ejecutar los notebooks en Databricks.

---

## 0:00 – 0:40 · Problema y pregunta (C)

**En pantalla:** README del repositorio en GitHub.

- C: "Cuando una entidad pública firma un contrato en SECOP II, muchas veces después
  le añade días. Eso es una prórroga."
- C: "Nuestra pregunta: con lo que se sabe **el día de la firma**, ¿podemos anticipar
  si ese contrato se va a prorrogar?"
- C: "Es un problema desbalanceado: en 2019, cerca del 11 % de los contratos tuvo
  prórroga."

## 0:40 – 1:20 · Datos y privacidad (JP)

**En pantalla:** `src/secop/esquema.py` (lista `COLUMNAS_PERSONALES`) y el notebook 01.

- JP: "Usamos la API abierta de datos.gov.co, el conjunto de contratos electrónicos de
  SECOP II: 95 columnas y millones de filas. Solo en 2019 son 142.090 contratos."
- JP: "Descargamos por año y mes, con reintentos y reanudación, porque la API a veces
  es lenta."
- JP: "Hay columnas con datos personales: nombres, documentos y cuentas bancarias.
  Ni siquiera las pedimos a la API: usamos `$select` con las columnas permitidas. Y si
  llegan en una descarga antigua, se eliminan antes de escribir la primera tabla."

## 1:20 – 2:10 · Arquitectura bronce, plata, oro y Delta (C)

**En pantalla:** diagrama mermaid del README; luego `DESCRIBE HISTORY` en el notebook 02.

- C: "Organizamos los datos en tres capas. **Bronce**: copia fiel, todo como texto."
- C: "**Plata**: diez tablas en tercera forma normal. Por ejemplo, la modalidad depende
  del proceso de compra y no del contrato, así que vive en `dim_proceso`; y esas
  dependencias no las suponemos: las medimos con los datos."
- C: "Separamos lo que se sabe al firmar, en la
  tabla `contrato`, de lo que pasa después, en `contrato_ejecucion`."
- C: "**Oro**: una fila por contrato, con la etiqueta y solo variables conocidas al firmar."
- C: "Todo se guarda en tablas Delta. Aquí se ve el historial: cada carga es una versión
  y podemos volver atrás con *time travel*."
- C: "Al recargar un año usamos `replaceWhere`: solo se reemplaza esa partición. Repetir
  el notebook no duplica filas."

## 2:10 – 2:50 · Calidad y pruebas (JP)

**En pantalla:** pull request en GitHub con la CI en verde; luego la carpeta `tests/`.

- JP: "La lógica no vive en los notebooks: vive en `src/secop`, en funciones pequeñas
  que probamos con pytest y datos sintéticos que imitan los casos raros reales."
- JP: "Cada push ejecuta GitHub Actions: compila, revisa estilo con ruff, corre las
  pruebas y construye la imagen de Docker."
- JP: "Con Codespaces cualquiera abre el mismo entorno, y con `make simular` corremos
  los notebooks completos en local, con Delta y datos sintéticos, antes de ir a
  Databricks."

## 2:50 – 3:50 · Modelo (C)

**En pantalla:** notebook 06 y el experimento en MLflow.

- C: "Partimos por **fecha de firma**: entrenamos con el pasado y evaluamos con el futuro,
  como se usaría en la vida real."
- C: "Por el desbalance usamos pesos de clase, y no medimos exactitud: un modelo que
  siempre diga 'no' acierta casi 89 % y no sirve. Usamos AUC-ROC, AUC-PR y F1."
- C: "Comparamos una regresión logística, que es la línea base, con árboles potenciados."
- C: "Una duda real: no sabemos si la fecha de fin ya incluye la prórroga. Por eso
  entrenamos con y sin el plazo. Si el AUC salta de forma sospechosa, hay fuga."
- C: "Todo queda registrado en MLflow: parámetros, métricas y versión del código."

## 3:50 – 4:30 · Resultados (JP)

**En pantalla:** tabla comparativa del notebook 06 y gráficos del notebook 05.

- JP: "En prueba, el mejor modelo sin plazo logra un AUC-ROC de [PENDIENTE: AUC-ROC prueba]
  y un AUC-PR de [PENDIENTE: AUC-PR prueba], frente a [PENDIENTE: tasa de positivos en
  prueba] de un modelo al azar."
- JP: "Con el umbral elegido en validación, el F1 es [PENDIENTE: F1 prueba]."
- JP: "Al añadir el plazo, el AUC cambia en [PENDIENTE: ΔAUC con plazo], lo que
  interpretamos como [PENDIENTE: interpretación de fuga]."
- JP: "Descriptivamente, la tasa de prórroga es más alta en [PENDIENTE: grupos con mayor
  tasa según el notebook 05]."

## 4:30 – 5:00 · Cierre y limitaciones (C y JP)

- C: "En resumen: un pipeline reproducible en la nube, con datos versionados, pruebas
  automáticas y experimentos registrados."
- JP: "Limitaciones: no confirmamos si la fecha de fin incluye la prórroga, solo usamos los
  años descargados y los años recientes no sirven para entrenar porque aún pueden
  recibir prórrogas."
- C: "La base todavía la debe confirmar el profesor; si cambia, se ajustan sobre todo
  `esquema.py` y `config.py`. Gracias."

---

## Reparto sugerido de preguntas

| Tema | Responde |
|---|---|
| Spark: lazy evaluation, acciones, inmutabilidad, broadcast, serverless | Cristian |
| Delta Lake, time travel, `replaceWhere`, capas bronce/plata/oro | Cristian |
| Modelo: desbalance, métricas, partición temporal, fuga, MLflow | Cristian (JP apoya) |
| Datos, API Socrata, privacidad, limpieza | JP |
| GitHub, ramas, PR, CI, Docker, Codespaces, reproducibilidad | JP |
| Resultados y limitaciones | Quien presentó ese bloque |

Ver respuestas preparadas en `docs/preguntas_probables.md`.
