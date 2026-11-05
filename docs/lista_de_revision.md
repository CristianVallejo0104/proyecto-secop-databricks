# Lista de revisión final (Fase 6)

Resultado real de cada comprobación, hecha el 2026-11-05 sobre la rama
`trabajo/proyecto-completo`. "✅" = verificado ejecutando; "⚠️" = verificado solo
leyendo el código o con una salvedad; "❌" = no se pudo verificar.

| # | Punto | Resultado | Cómo se verificó |
|---|---|---|---|
| 1 | Sin datos personales | ✅ | Los nombres de columnas personales solo aparecen en `src/secop/esquema.py` (búsqueda en todo el repo + test `test_nombres_de_columnas_personales_solo_en_esquema`). Bronce descarta esas columnas; un test verifica que ninguna tabla de plata ni oro tenga esas columnas ni el marcador sintético. La descarga (01) pide solo columnas permitidas con `$select`. |
| 2 | Sin variables posteriores a la firma | ✅ / ⚠️ | `contrato` y `gold_contratos` no contienen columnas de `COLUMNAS_POSTERIORES_FIRMA` (tests). Del lado de ejecución, oro solo toma `dias_adicionados` para la etiqueta. ⚠️ Plazo y duración quedan como variante de sensibilidad (no en el modelo principal). Ver riesgos. |
| 3 | Sin `cache`/`persist`/RDD/`sparkContext` | ✅ | `git grep` sin resultados en ningún `.py` (los tests también los evitan: el nivel de log se fija con `spark.log.level`). |
| 4 | Sin `toPandas()` sobre tablas grandes | ✅ | Único uso en notebooks: plan B de `06_modelo`, sobre una muestra limitada con `sample(...).limit(max_filas_muestra)` y unas 30 columnas. Otro uso en `tests/test_modelos.py` sobre 400 filas sintéticas. Los descriptivos agregan en Spark. |
| 5 | Sin credenciales | ✅ | Búsqueda de patrones de tokens, contraseñas y llaves sin resultados; `.gitignore` excluye `.env`, `.databrickscfg`, `*.pem`, `*.key`. |
| 6 | Sin datos ni archivos grandes en el repo | ✅ | No hay CSV/Parquet/JSON de datos versionados (el único `.json` es `devcontainer.json`). Archivo más grande: ~16 KB. |
| 7 | Sin cifras inventadas | ✅ | Las cifras de los documentos son solo los hechos de partida (142.090, 356.623, 141.257, 10,99 %, ~11 %, 5,67 millones, p99 ≈ 2,17e9, etc.) o derivadas directas (89 % = 100 % − 11 %). Todos los resultados dicen PENDIENTE. Las métricas de la simulación local (datos aleatorios) no se copiaron a ningún documento. |
| 8 | `py_compile` en todos los `.py` | ✅ | 26 archivos compilan. |
| 9 | Tests | ✅ | `pytest`: 54 pasan (PySpark 4.0.1, Java 21, modo ANSI, datos sintéticos). |
| 10 | ruff | ✅ | `ruff check .` y `ruff format --check .` sin errores. |
| 11 | Notebooks 02-06 de punta a punta | ✅ (local) | `scripts/simular_local.py` con Delta 4.0.0 y datos sintéticos: los 5 notebooks se ejecutan sin errores con `motor=auto` (MLlib) y con `motor=sklearn` (plan B). |
| 12 | Notebook 01 (descarga) | ⚠️ | Solo revisión de código y `py_compile`: no hay acceso a la API desde el entorno de desarrollo. |
| 13 | Docker | ✅ | Imagen construida y ejecutada en local; "Validación del esquema: OK". También corre en la CI. |
| 14 | GitHub Actions | ✅ | 4 ejecuciones en GitHub (fases 4 y 5, ambas ramas): todas exitosas. |
| 15 | Codespaces (`devcontainer.json`) | ⚠️ | Revisado; no se abrió un Codespace real. |
| 16 | Ejecución en Databricks serverless | ❌ | No es posible desde aquí. La deben hacer los autores con `docs/runbook_databricks.md`. |
| 17 | Commits por fase y PR hacia `main` sin merge | ✅ | Un commit por fase (0 a 6), push con pull previo; PR abierto sin merge. |
| 18 | Sin etiquetas de release | ✅ | No se creó ninguna etiqueta; el plan está en `docs/flujo_git.md`. |

## Riesgos conocidos

1. **Serverless no probado:** la simulación local usa Spark clásico, no Spark Connect.
   Puntos con más riesgo: el soporte de MLlib en serverless (hay plan B automático),
   `mlflow.spark.log_model` (protegido con `try`), y `_metadata.file_name` al leer CSV.
2. **Plazo y duración:** no se sabe si la fecha de fin ni la duración se actualizan con
   la prórroga. Por eso se comparan tres variantes; el umbral de 0,05 en el AUC es una
   regla práctica, no una prueba formal.
3. **Otras variables posiblemente actualizadas:** `puede_ser_prorrogado` (cláusula del
   contrato) y `fecha_inicio` se tratan como conocidas al firmar, pero SECOP podría
   actualizarlas después. Conviene revisar su efecto en el modelo (si una sola de
   ellas explica casi todo, sospechar).
4. **CSV antiguos con datos personales:** los archivos de 2019-2020 ya descargados en el
   Volume conservan esas columnas; bronce las descarta, pero se recomienda volver a
   descargarlos (runbook §4).
5. **Cambio de esquema entre descargas:** las descargas nuevas tienen 75 columnas y las
   antiguas 95; 02 detecta la mezcla dentro de un mismo año y se detiene.
6. **Partición temporal con solo 2019-2020:** la prueba (segundo semestre de 2020)
   coincide con la pandemia; los resultados podrían no generalizar.
7. **Filtro de descarga por fecha de firma:** los contratos sin fecha de firma no se descargan.
8. **Rama:** el trabajo se publicó en `trabajo/proyecto-completo` (la pedida) y también
   en `claude/compassionate-rubin-802hzw` (rama asignada al entorno de desarrollo);
   ambas tienen los mismos commits.
