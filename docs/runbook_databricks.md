# Runbook: ejecutar el proyecto en Databricks Free Edition

Paso a paso para ir de cero a un modelo registrado en MLflow. Tiempo estimado:
PENDIENTE (anotar después de la primera ejecución completa).

## 0. Requisitos

- Cuenta de Databricks Free Edition (cómputo *serverless*).
- Acceso al repositorio en GitHub.
- Unity Catalog con el catálogo `workspace` (viene por defecto en Free Edition).

## 1. Crear el esquema y el Volume (una sola vez)

En un notebook o en el editor SQL:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.secop;
CREATE VOLUME IF NOT EXISTS workspace.secop.raw;
```

- Línea 1: crea el esquema (la "carpeta" lógica de tablas) `secop` dentro del catálogo `workspace`.
- Línea 2: crea el Volume `raw`, almacenamiento de archivos administrado por Unity
  Catalog. Su ruta es `/Volumes/workspace/secop/raw/`.

## 2. Enlazar el repositorio como Git folder

1. En la barra lateral: **Workspace** → tu carpeta de usuario → **Create** → **Git folder**.
2. Pegar la URL del repositorio de GitHub y elegir la rama (por ejemplo `main`).
3. Si es privado, conectar la cuenta de GitHub en **Settings → Linked accounts**.

Por qué Git folder: los notebooks y `src/secop` quedan sincronizados con GitHub; los
cambios se traen con **Pull** desde el menú Git (regla de oro: pull antes de editar).

**Import de `src/secop`:** cada notebook busca la carpeta `src/secop` subiendo
desde su carpeta y la añade a `sys.path`. Si falla, escribe la ruta del repo en el
widget `ruta_repo` (por ejemplo `/Workspace/Users/tu_correo/proyecto-secop-databricks`).
Si editas un archivo de `src/`, ejecuta `dbutils.library.restartPython()` para que
Python lo vuelva a cargar.

## 3. Orden de ejecución

Cada notebook es autocontenido: lee tablas, no variables de otro notebook. Se
pueden ejecutar uno por uno o encadenarlos en un *Job*.

| # | Notebook | Entrada | Salida | Widgets clave |
|---|---|---|---|---|
| 1 | `01_descarga_exploracion` | API datos.gov.co | CSV en `/Volumes/.../contratos/anio=AAAA/` | `anios`, `modo` |
| 2 | `02_bronce` | CSV del Volume | `workspace.secop.bronze_contratos` | `anios`, `recrear_tabla` |
| 3 | `03_plata` | bronce | 6 tablas de plata | — |
| 4 | `04_oro` | plata | `workspace.secop.gold_contratos` | — |
| 5 | `05_descriptivo` | oro | tablas en pantalla | `min_contratos` |
| 6 | `06_modelo` | oro | experimento MLflow `/Users/<tú>/secop_prorrogas` | cortes, `motor`, `commit` |

### Verificaciones esperadas

| Paso | Qué revisar | Valor de referencia |
|---|---|---|
| 01 | Filas locales vs servidor | 2019: 142.090; 2020: 356.623 |
| 02 | Filas por `anio_carga`; columnas sin datos personales | Igual que 01 |
| 03 | Integridad referencial | 0 huérfanas en las 5 relaciones (si no, el notebook se detiene) |
| 04 | Filas y tasa de 2019 | Referencia previa: 141.257 filas y 10,99 % |
| 06 | Runs en MLflow | 1 run padre + 6 hijos (3 variantes × 2 algoritmos) |

## 4. Datos personales en descargas antiguas

Los CSV de 2019 y 2020 ya descargados traen las 95 columnas, incluidas las
personales. Bronce las descarta, pero los archivos siguen en el Volume. Para
eliminarlas también del almacenamiento:

1. Borrar la carpeta del año: `dbutils.fs.rm("/Volumes/workspace/secop/raw/contratos/anio=2019", True)`.
2. Ejecutar `01` con `anios=2019` y `modo=descargar` (ahora pide solo columnas permitidas).
3. Ejecutar `02` con `anios=2019`.

**Cuidado:** si alguna vez se escribió una tabla con esas columnas, las versiones
antiguas siguen en el historial de Delta. Para borrarlas: recrear la tabla
(`recrear_tabla = si`) y, si aplica, `VACUUM` según la política de retención.

## 5. Errores previsibles y solución

### Partición `anio_carga`

| Síntoma | Causa | Solución |
|---|---|---|
| `replaceWhere` falla o dice que la tabla no está particionada por `anio_carga` | La tabla bronce se creó antes con otro esquema o sin partición | Ejecutar `02` con `recrear_tabla = si` |
| Aparece una columna `anio` además de `anio_carga` | Se leyó la carpeta padre `.../contratos`: Spark interpreta `anio=2019` como partición | Leer cada año por separado (así lo hace `02`) |
| Esquema distinto al escribir (`A schema mismatch detected`) | Versión anterior de bronce con más columnas | `recrear_tabla = si` |
| "Los CSV tienen encabezados distintos" | En una misma carpeta hay descargas antiguas (95 columnas) y nuevas (sin personales) | Borrar el año y descargarlo completo de nuevo |

### API de datos.gov.co

| Síntoma | Solución |
|---|---|
| `Timeout` o `HTTP 5xx/429` | El notebook reintenta solo (5, 10, 20, 40... s). Si agota los intentos, volver a ejecutar: reanuda donde quedó |
| Diferencia grande entre filas locales y del servidor | Borrar `mes_MM.listo` y las partes de ese mes, y volver a descargar |
| Diferencia pequeña | Contratos actualizados en el servidor después de descargar: anotar y seguir |
| Contratos sin fecha de firma | No se descargan (el filtro es por fecha de firma). Limitación documentada |

### Límites de serverless (Free Edition)

| Síntoma | Causa | Solución |
|---|---|---|
| Error al usar `cache()`, `persist()`, `sparkContext` o RDD | No soportados en serverless | El código no los usa; no agregarlos |
| `OutOfMemory` en el driver | `toPandas()` o `collect()` sobre muchas filas | Agregar en Spark y mostrar resultados pequeños; en el plan B bajar `max_filas_muestra` |
| Error de MLlib en `06` | Alguna etapa no disponible en serverless | `motor = auto` cambia solo a scikit-learn y lo registra en MLflow (`motivo_plan_b`) |
| No se guarda el modelo en MLflow | Spark ML en serverless requiere un directorio temporal en un Volume | Las métricas se registran igual; el motivo queda en la etiqueta `modelo_no_guardado` |
| `CAST_INVALID_INPUT` | Modo ANSI con un `cast` normal | Usar las funciones de `src/secop/limpieza.py` (usan `try_cast`) |
| La sesión se reinicia o expira | Inactividad del cómputo serverless | Volver a ejecutar el notebook: todos son idempotentes |

## 6. Después de ejecutar

1. Copiar los valores de verificación y las métricas a `docs/informe.md`
   (reemplazar cada PENDIENTE; nunca inventar).
2. En `06`, escribir el commit de git en el widget `commit` para que quede en MLflow.
3. Crear la etiqueta de versión correspondiente (ver `docs/flujo_git.md`).

## 7. Probar sin Databricks

- `make test`: 54 pruebas con Spark local y datos sintéticos.
- `make simular`: ejecuta los notebooks 02 a 06 en local con Delta y datos sintéticos
  (requiere `pip install -r requirements-simulacion.txt`).
- `make docker`: valida el esquema dentro de un contenedor.
