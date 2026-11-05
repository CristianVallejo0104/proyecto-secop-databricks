"""Entorno de ejecución local: Spark con Delta, tablas, MLflow y parámetros.

Este módulo reemplaza lo que antes daba una plataforma administrada: crea la
sesión de Spark, guarda y lee tablas Delta en ``datos/delta/`` y configura
MLflow en ``datos/mlruns/``. Así los notebooks quedan cortos y todos usan las
mismas opciones.

Spark corre en MODO LOCAL: el driver y los ejecutores son hilos dentro de un
mismo proceso, en el Codespace. Es el mismo motor y la misma API (PySpark) que
en un clúster; solo cambia que el trabajo se reparte entre los núcleos de una
máquina en vez de entre varias máquinas.
"""

import os
import shutil
from pathlib import Path

from secop import config


# ---------------------------------------------------------------------------
# Parámetros
# ---------------------------------------------------------------------------
def parametro(nombre: str, defecto: str) -> str:
    """Valor de un parámetro: la variable de entorno SECOP_<NOMBRE> o el valor por defecto.

    Ejemplo: ``parametro("ANIOS", "2019,2020")`` devuelve "2019,2020", salvo que
    se haya definido ``SECOP_ANIOS=2021`` antes de abrir el notebook. Así el
    script que ejecuta los notebooks puede cambiar parámetros sin editarlos.
    """
    return os.environ.get(f"SECOP_{nombre.upper()}", defecto)


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
def crear_spark(nombre_app: str = "secop"):
    """Crea (o reutiliza) la sesión de Spark local con Delta Lake.

    Configuración y por qué:
    - ``local[*]``: usa todos los núcleos del Codespace.
    - ``spark.driver.memory``: en modo local el driver hace todo el trabajo; por
      defecto 4 GB (variable SECOP_MEMORIA). Debe fijarse ANTES de iniciar Spark.
    - Extensiones de Delta: permiten ``format("delta")``, ``replaceWhere`` y
      ``DESCRIBE HISTORY``.
    - Modo ANSI: un cast inválido falla en vez de inventar un valor; por eso
      todo el código usa ``try_cast``.
    - Pocas particiones de shuffle (8): con un solo computador, 200 (el valor
      por defecto) crearía demasiadas tareas pequeñas.
    """
    from pyspark.sql import SparkSession

    constructor = (
        SparkSession.builder.master(os.environ.get("SECOP_SPARK_MASTER", "local[*]"))
        .appName(nombre_app)
        .config("spark.driver.memory", os.environ.get("SECOP_MEMORIA", "4g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SECOP_PARTICIONES", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(config.raiz_datos() / "spark-warehouse"))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.log.level", "ERROR")
    )
    # Los jars de Delta se descargan de Maven la primera vez. Si la red lo
    # impide, se pueden descargar a mano y poner su carpeta en SECOP_JARS_DELTA.
    jars = os.environ.get("SECOP_JARS_DELTA")
    if jars:
        lista = ",".join(str(p) for p in sorted(Path(jars).glob("*.jar")))
        return constructor.config("spark.jars", lista).getOrCreate()
    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(constructor).getOrCreate()


# ---------------------------------------------------------------------------
# Tablas Delta (guardadas por ruta, en datos/delta/<nombre>)
# ---------------------------------------------------------------------------
def tabla_existe(nombre: str) -> bool:
    """Una carpeta es tabla Delta si tiene el registro de transacciones _delta_log."""
    return (Path(config.ruta_tabla(nombre)) / "_delta_log").is_dir()


def leer_tabla(spark, nombre: str):
    """Lee una tabla Delta. No carga datos: Spark solo arma el plan (lazy)."""
    if not tabla_existe(nombre):
        raise FileNotFoundError(
            f"No existe la tabla '{nombre}' en {config.ruta_tabla(nombre)}. "
            "Ejecuta antes el notebook que la crea."
        )
    return spark.read.format("delta").load(config.ruta_tabla(nombre))


def leer_version(spark, nombre: str, version: int):
    """Time travel: lee la tabla tal como estaba en una versión anterior."""
    return spark.read.format("delta").option("versionAsOf", version).load(config.ruta_tabla(nombre))


def escribir_tabla(
    df,
    nombre: str,
    particion: str | None = None,
    reemplazar_donde: str | None = None,
) -> None:
    """Escribe un DataFrame como tabla Delta (sobrescribe).

    - Con ``reemplazar_donde`` (p. ej. "anio_carga = 2019") y la tabla ya
      creada: reemplaza SOLO las filas que cumplen la condición (replaceWhere);
      el resto de la tabla queda intacto.
    - Si no: reemplaza la tabla completa y acepta un esquema nuevo
      (overwriteSchema), particionada por ``particion`` si se indica.
    La escritura es atómica: si falla a mitad, la tabla queda como estaba.
    """
    escritor = df.write.format("delta").mode("overwrite")
    if reemplazar_donde and tabla_existe(nombre):
        escritor = escritor.option("replaceWhere", reemplazar_donde)
    else:
        escritor = escritor.option("overwriteSchema", "true")
        if particion:
            escritor = escritor.partitionBy(particion)
    escritor.save(config.ruta_tabla(nombre))


def borrar_tabla(nombre: str) -> None:
    """Elimina una tabla Delta y TODO su historial (no se puede deshacer)."""
    shutil.rmtree(config.ruta_tabla(nombre), ignore_errors=True)


def historial(spark, nombre: str):
    """Versiones de la tabla (DESCRIBE HISTORY): qué operación, cuándo y con qué parámetros."""
    return spark.sql(f"DESCRIBE HISTORY delta.`{config.ruta_tabla(nombre)}`")


# ---------------------------------------------------------------------------
# Mostrar resultados en Jupyter
# ---------------------------------------------------------------------------
def mostrar(df, filas: int = 20):
    """Muestra las primeras filas de un DataFrame de Spark como tabla.

    Solo trae ``filas`` filas a la memoria (con limit ANTES de toPandas): nunca
    la tabla completa. Úsese con resultados ya agregados o para echar un vistazo.
    """
    tabla = df.limit(filas).toPandas()
    try:
        from IPython.display import display

        display(tabla)
    except ImportError:  # fuera de Jupyter
        print(tabla.to_string())
    return None


# ---------------------------------------------------------------------------
# MLflow
# ---------------------------------------------------------------------------
def configurar_mlflow(experimento: str = config.EXPERIMENTO_MLFLOW):
    """Guarda los experimentos de MLflow en datos/mlruns y activa el experimento.

    Para ver los resultados en el navegador: ``make mlflow`` (abre la interfaz).
    """
    import mlflow

    config.ruta_mlflow().mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(config.ruta_mlflow().resolve().as_uri())
    mlflow.set_experiment(experimento)
    return mlflow


def version_git() -> dict[str, str]:
    """Commit actual del repositorio y si hay cambios sin commit ("sucio").

    Se registra en MLflow: así cada modelo queda ligado a una versión exacta del
    código. Si hay cambios sin commit, el resultado NO es reproducible desde git.
    """
    import subprocess

    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=config.RAIZ_REPO,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        estado = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=config.RAIZ_REPO,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        return {"commit_git": commit, "git_con_cambios_sin_commit": str(bool(estado))}
    except (OSError, subprocess.CalledProcessError):
        return {"commit_git": "desconocido", "git_con_cambios_sin_commit": "desconocido"}
