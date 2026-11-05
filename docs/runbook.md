# Runbook: ejecutar el proyecto en GitHub Codespaces

Paso a paso para ir de cero a un modelo registrado en MLflow, todo en Python
(PySpark en modo local) dentro de un Codespace. Tiempo estimado: PENDIENTE
(anotar después de la primera ejecución completa).

## 0. Qué es cada pieza

| Pieza | Qué hace aquí |
|---|---|
| **Codespace** | Un computador en la nube con VS Code, Python 3.12 y Java 17 ya instalados |
| **PySpark (modo local)** | Spark corre dentro del Codespace y reparte el trabajo entre sus núcleos |
| **Delta Lake** | Formato de las tablas, guardadas en `datos/delta/` (transacciones e historial) |
| **MLflow** | Registro de experimentos en `datos/mlruns/`, con interfaz web |
| **Carpeta `datos/`** | Todo lo que generan los notebooks. Está en `.gitignore`: nunca se sube |

## 1. Crear el Codespace (una sola vez por persona)

1. En GitHub, abrir el repositorio y elegir la rama de trabajo (o `main` si ya se
   hizo el merge).
2. Botón verde **Code** → pestaña **Codespaces** → **⋯** → **New with options**.
3. En **Machine type**, elegir **4-core** para el pipeline con datos reales (2-core
   alcanza para tests y datos sintéticos, pero el modelo será lento).
4. **Create codespace**. La primera vez tarda unos minutos: `.devcontainer/devcontainer.json`
   instala Java 17, las dependencias con versiones fijas y valida el esquema.
5. En la terminal, comprobar que todo funciona:

```bash
make test
```

Debe terminar con todas las pruebas en verde (`passed`).

**Cuidado con el cupo:** Codespaces tiene horas gratuitas al mes (las de 4 núcleos
consumen el doble que las de 2). Al terminar, **detener** el Codespace:
**Code** → **Codespaces** → **⋯** → **Stop codespace**. No se pierde nada al
detenerlo; sí se pierde al **borrarlo** (incluida la carpeta `datos/`).

## 2. Probar el pipeline con datos sintéticos (5 minutos)

Antes de descargar nada, comprobar que los notebooks corren de punta a punta:

```bash
make notebooks-prueba
```

Ejecuta los notebooks 02 a 06 sobre una carpeta temporal con datos inventados. No
toca `datos/`. Las métricas que salen no significan nada.

## 3. Ejecutar con datos reales

Dos formas equivalentes:

**a) Interactiva (recomendada la primera vez y para la sustentación):** abrir cada
notebook en VS Code (carpeta `notebooks/`), elegir el kernel **Python 3.12** y
pulsar **Run All**, en este orden:

| # | Notebook | Entrada | Salida |
|---|---|---|---|
| 1 | `01_descarga_exploracion` | API datos.gov.co | CSV en `datos/raw/contratos/anio=AAAA/` |
| 2 | `02_bronce` | CSV | tabla `bronze_contratos` |
| 3 | `03_plata` | bronce | 10 tablas en 3FN |
| 4 | `04_oro` | plata | tabla `gold_contratos` |
| 5 | `05_descriptivo` | oro | tablas y gráficos |
| 6 | `06_modelo` | oro | experimento MLflow `secop_prorrogas` |

**b) Todo de una vez desde la terminal:**

```bash
make pipeline
```

Ejecuta 01 a 06 en orden y guarda copias con los resultados en `datos/ejecutados/`
(se abren en VS Code como cualquier notebook). Para empezar desde un notebook:

```bash
python scripts/ejecutar_notebooks.py --desde 03
```

Cada notebook es autocontenido: lee tablas de `datos/`, no variables de otro
notebook. Se puede repetir cualquiera sin duplicar datos.

### Parámetros

Cada notebook tiene una celda **Parámetros**. Se pueden cambiar ahí o con
variables de entorno antes de ejecutar, por ejemplo:

```bash
SECOP_ANIOS=2019,2020,2021 make pipeline
```

| Variable | Notebook | Por defecto | Para qué |
|---|---|---|---|
| `SECOP_ANIOS` | 01, 02 | `2019,2020` | Años a descargar y cargar |
| `SECOP_MODO` | 01 | `descargar` | `solo_verificar` para no descargar |
| `SECOP_RECREAR` | 02 | `no` | `si` borra bronce y la crea de nuevo |
| `SECOP_MIN_CONTRATOS` | 05 | `100` | Tamaño mínimo de grupo en los descriptivos |
| `SECOP_MOTOR` | 06 | `auto` | `mllib` o `sklearn` para forzar uno |
| `SECOP_ITER_GBT` | 06 | `40` | Árboles del GBT (menos = más rápido) |
| `SECOP_CORTE_VAL`, `SECOP_CORTE_PRU` | 06 | `2020-04-01`, `2020-07-01` | Partición temporal |
| `SECOP_MEMORIA` | todos | `4g` | Memoria de Spark (subir a `10g` en una máquina de 16 GB) |
| `SECOP_DATOS` | todos | `datos/` | Carpeta de datos |

### Verificaciones esperadas

| Paso | Qué revisar | Valor de referencia |
|---|---|---|
| 01 | Filas locales vs servidor | 2019: 142.090; 2020: 356.623 |
| 02 | Filas por `anio_carga`; columnas sin datos personales | Igual que 01 |
| 03 | Dependencias funcionales | "entidad → nombre": 8 violaciones en 2019; el resto, anotar en el informe |
| 03 | Llaves primarias | Únicas y sin nulos en las 10 tablas (si no, el notebook se detiene) |
| 03 | Integridad referencial | 0 huérfanas en las 10 relaciones (si no, el notebook se detiene) |
| 04 | Filas y tasa de 2019 | Referencia previa: 141.257 filas y 10,99 % |
| 06 | Runs en MLflow | 1 run padre + 6 hijos (3 variantes × 2 algoritmos) |

## 4. Ver los experimentos en MLflow

```bash
make mlflow
```

Codespaces muestra un aviso para abrir el puerto 5000 en el navegador (también en
la pestaña **Ports**). Ahí se comparan los runs: parámetros, métricas, commit de
git y si había cambios sin commit. `Ctrl+C` en la terminal para cerrarlo.

Mientras un notebook corre, la interfaz de Spark (puerto 4040) muestra los trabajos,
etapas y tareas: útil para explicar cómo Spark divide el trabajo.

## 5. Datos personales: descargas antiguas

La descarga del notebook 01 pide a la API solo columnas permitidas. Si en algún
momento se descargaron CSV completos (por ejemplo, la descarga anterior de 2019 y
2020 en Databricks), esos archivos contienen columnas personales: **bórralos donde
estén** y descarga de nuevo con el notebook 01. Bronce de todas formas descarta
esas columnas.

Si alguna vez se escribió una tabla Delta con esas columnas, sus versiones
antiguas siguen en el historial. Para eliminarlas: ejecutar 02 con
`SECOP_RECREAR=si`, que borra la tabla con todo su historial.

## 6. Errores previsibles y solución

| Síntoma | Causa | Solución |
|---|---|---|
| `JAVA_GATEWAY_EXITED` o "Java not found" | No hay Java 17 | Recrear el Codespace (el devcontainer lo instala) o `sudo apt install openjdk-17-jdk` |
| Error al descargar `io.delta#delta-spark` la primera vez | Spark baja los jars de Delta desde Maven y no hubo red | Reintentar; si persiste, descargar los jars y definir `SECOP_JARS_DELTA` con su carpeta |
| `OutOfMemoryError` / el kernel muere | Poca memoria para Spark | Máquina de 4 núcleos y `SECOP_MEMORIA=10g`; en 06, bajar `SECOP_ITER_GBT` o usar `SECOP_MOTOR=sklearn` |
| `No module named 'secop'` | El notebook no encontró `src/` | Abrir el notebook desde la carpeta del repo; reiniciar el kernel |
| Cambié código en `src/` y no se nota | Python ya importó la versión anterior | **Restart** del kernel y ejecutar de nuevo |
| `No existe la tabla ...` | Se saltó un notebook | Ejecutar los notebooks en orden (01 → 06) |
| "Los CSV tienen encabezados distintos" | Mezcla de descargas antiguas y nuevas en un año | Borrar `datos/raw/contratos/anio=AAAA/` y descargar de nuevo |
| `replaceWhere` o esquema distinto en 02 | Bronce existe con otro esquema | `SECOP_RECREAR=si` en 02 |
| `Timeout` o `HTTP 5xx/429` en 01 | La API está lenta | El notebook reintenta solo; si agota los intentos, volver a ejecutar: reanuda donde quedó |
| Diferencia grande entre filas locales y servidor | Un mes quedó incompleto | Borrar su `mes_MM.listo` y sus partes; descargar de nuevo |
| `CAST_INVALID_INPUT` | Un `cast` normal en modo ANSI | Usar las funciones de `src/secop/limpieza.py` (usan `try_cast`) |
| La CI falla en `test_notebooks_se_suben_sin_salidas` | Se subió un notebook con salidas | **Clear All Outputs** y commit de nuevo |
| Se llenó el disco del Codespace | Muchos años descargados | Borrar años que no se usen de `datos/raw/`; las tablas Delta se pueden regenerar |

## 7. Después de ejecutar

1. Copiar los valores de verificación y las métricas a `docs/informe.md`
   (reemplazar cada PENDIENTE; nunca inventar).
2. **Clear All Outputs** en los notebooks antes de hacer commit.
3. Crear la etiqueta de versión correspondiente (ver `docs/flujo_git.md`).
