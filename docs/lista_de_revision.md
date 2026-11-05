# Lista de revisión

Resultado real de cada comprobación, hecha el 2026-11-05 sobre la rama
`trabajo/proyecto-completo`, después de pasar de Databricks a Codespaces con
PySpark local y notebooks Jupyter. "✅" = verificado ejecutando; "⚠️" = verificado
solo leyendo el código o con una salvedad; "❌" = no se pudo verificar.

| # | Punto | Resultado | Cómo se verificó |
|---|---|---|---|
| 1 | Sin datos personales | ✅ | Los nombres de columnas personales solo aparecen en `src/secop/esquema.py` (test `test_nombres_de_columnas_personales_solo_en_esquema`, que revisa todos los archivos, incluidos los `.ipynb`). Bronce descarta esas columnas; un test verifica que ninguna tabla de plata ni oro las tenga. La descarga pide solo columnas permitidas (`$select`), probado con una API falsa. |
| 2 | Sin variables posteriores a la firma | ✅ / ⚠️ | `contrato` y `gold_contratos` no contienen columnas de `COLUMNAS_POSTERIORES_FIRMA` (tests). Oro solo toma `dias_adicionados` para la etiqueta. ⚠️ Plazo y duración quedan como variante de sensibilidad (no en el modelo principal). |
| 3 | Notebooks sin salidas en GitHub | ✅ | Test `test_notebooks_se_suben_sin_salidas` en los 6 `.ipynb`. |
| 4 | Sin `toPandas()` sobre tablas grandes | ✅ | `entorno.mostrar` hace `limit` antes de `toPandas()`; los gráficos usan resultados agregados con `limit`; el plan B de 06 usa una muestra con `sample(...).limit(MAX_FILAS)`. Un test comprueba que `mostrar` no trae la tabla completa. |
| 5 | Sin `cache`/`persist`/RDD/`sparkContext` en los notebooks | ✅ | Test `test_notebooks_no_usan_apis_prohibidas`. No se necesitan: cada capa se materializa en Delta. |
| 6 | Sin credenciales | ✅ | Búsqueda de patrones de tokens, contraseñas y llaves sin resultados; `.gitignore` excluye `.env`, `*.pem`, `*.key`. |
| 7 | Sin datos ni archivos grandes en el repo | ✅ | No hay CSV/Parquet/JSON de datos versionados (el único `.json` es `devcontainer.json`). `datos/` está en `.gitignore` (test). Archivo más grande: ~24 KB. |
| 8 | Sin cifras inventadas | ✅ | Las cifras de los documentos son solo los hechos de partida (142.090, 356.623, 141.257, 10,99 %, ~11 %, 5,67 millones, p99 ≈ 2,17e9) o derivadas directas (89 % = 100 % − 11 %). Todos los resultados dicen PENDIENTE. Las métricas de las ejecuciones con datos sintéticos no se copiaron a ningún documento. |
| 9 | `py_compile` en todos los `.py` | ✅ | 25 archivos compilan. |
| 10 | Tests | ✅ | `pytest`: 111 pasan (PySpark 4.0.1, Java 21, modo ANSI, datos sintéticos). |
| 11 | ruff (incluye notebooks) | ✅ | `ruff check .` y `ruff format --check .` sin errores. |
| 12 | Notebooks 02-06 (`.ipynb`) de punta a punta | ✅ (sintéticos) | `scripts/ejecutar_notebooks.py --sinteticos` con Jupyter (nbclient), Delta 4.0.0 y MLflow: los 5 notebooks corren sin errores; gráficos revisados visualmente. La CI los ejecuta en cada push. |
| 13 | Notebook 01 contra la API real | ⚠️ | La lógica de descarga está probada con una API falsa (reintentos, reanudación, columnas); la conexión real no se probó (sin acceso a la API desde el entorno de desarrollo). |
| 14 | Docker | ✅ | Imagen construida y ejecutada en local; "Validación del esquema: OK". También corre en la CI. |
| 15 | GitHub Actions | ✅ | Los tres trabajos en verde en GitHub: pruebas (py_compile, ruff, validación, pytest), Docker y **notebooks** (ejecuta 02-06 con datos sintéticos, unos 8 minutos). Con límite de 30 minutos y de 20 minutos por celda para que nunca quede colgada. |
| 16 | Codespaces (`devcontainer.json`) | ⚠️ | Revisado; no se abrió un Codespace real desde aquí. |
| 17 | Ejecución con datos reales | ❌ | Pendiente: la deben hacer los autores con `make pipeline` (ver `docs/runbook.md`). |
| 18 | Plata en 1FN, 2FN y 3FN | ✅ (sintéticos) | 10 tablas. Tests por forma normal, unicidad y no nulidad de las 10 llaves primarias e integridad en las 10 relaciones (incluida la llave compuesta departamento + municipio). |
| 19 | Dependencias funcionales verificadas con datos | ✅ (sintéticos) / ❌ (reales) | `plata.revisar_dependencias` detecta los casos sembrados y da 0 en el resto. Con datos reales: PENDIENTE. |
| 20 | Reproducibilidad en MLflow | ✅ | Cada run guarda versión del paquete, commit de git y si había cambios sin commit (verificado en la ejecución con datos sintéticos). |
| 21 | Sin etiquetas de release | ✅ | No se creó ninguna etiqueta; el plan está en `docs/flujo_git.md`. |

## Riesgos conocidos

1. **Memoria y tiempo en el Codespace:** con los datos reales, el GBT puede tardar en una
   máquina de 2 núcleos. Mitigación: máquina de 4 núcleos, `SECOP_MEMORIA=10g`, bajar
   `SECOP_ITER_GBT` o usar el plan B (`SECOP_MOTOR=sklearn`).
2. **Jars de Delta:** Spark los descarga de Maven la primera vez; sin red falla
   (alternativa: `SECOP_JARS_DELTA`).
3. **Dependencias funcionales con datos reales:** el diseño 3FN supone, por ejemplo, que
   cada proceso de compra tiene una sola modalidad. Si hay muchas violaciones, la regla
   "valor más reciente" podría perder información; el notebook 03 muestra el conteo.
4. **Plazo y duración:** no se sabe si la fecha de fin ni la duración se actualizan con
   la prórroga. Se comparan tres variantes; el umbral de 0,05 en el AUC es una regla
   práctica, no una prueba formal. Con muestras pequeñas el AUC varía por azar más de
   0,05 (pasó con los datos sintéticos), así que con datos reales hay que mirar también
   el tamaño de la partición de prueba.
5. **Otras variables posiblemente actualizadas:** `puede_ser_prorrogado` y `fecha_inicio`
   se tratan como conocidas al firmar, pero SECOP podría actualizarlas después.
6. **CSV antiguos con datos personales:** la descarga anterior de 2019-2020 (en Databricks)
   tenía las 95 columnas. Hay que borrar esos archivos donde estén y descargar de nuevo
   con el notebook 01 (runbook §5).
7. **Codespace borrado = datos perdidos:** `datos/` vive solo en el Codespace. Detenerlo
   no borra nada; eliminarlo sí (habría que volver a descargar).
8. **Partición temporal con solo 2019-2020:** la prueba (segundo semestre de 2020)
   coincide con la pandemia; los resultados podrían no generalizar.
9. **Filtro de descarga por fecha de firma:** los contratos sin fecha de firma no se descargan.
10. **Rama:** el trabajo se publicó en `trabajo/proyecto-completo` (la del PR) y también
    en `claude/compassionate-rubin-802hzw` (rama asignada al entorno de desarrollo).
