"""Capa plata: modelo relacional en tercera forma normal (3FN).

Tablas que se construyen a partir de bronce (PK = llave primaria, FK = foránea):

    dim_departamento (departamento) <── dim_municipio (departamento, municipio)
                                                 ^
    dim_entidad (codigo_entidad) ── FK (departamento, municipio) ──┘
    dim_familia_unspsc (familia_unspsc) <── dim_categoria (codigo_categoria)
    dim_proveedor (codigo_proveedor)
    dim_proceso (proceso_de_compra)

    contrato (id_contrato) ── FK a dim_entidad, dim_proveedor, dim_categoria, dim_proceso
       ├── contrato_fuente_recurso (id_contrato, fuente)   1 a muchos
       └── contrato_ejecucion (id_contrato)                 1 a 1

Formas normales (explicación completa en docs/decisiones.md §3):
- 1FN: cada celda tiene un valor atómico y no hay grupos repetidos.
  "Colombia, Antioquia, Medellín" se separa; "6 Mes(es)" se separa en número y
  unidad; las 6 columnas de fuentes de recursos pasan a filas.
- 2FN: en las tablas con llave compuesta, cada atributo depende de TODA la llave.
- 3FN: ningún atributo depende de otro atributo que no sea llave (no hay
  dependencias transitivas). Ejemplo: segmento depende de familia, no del código
  de categoría; por eso vive en dim_familia_unspsc.
- No se guardan datos calculables a partir de otros (p. ej. días de duración):
  se calculan en oro.

Separación clave: ``contrato`` tiene SOLO lo que se conoce al firmar; lo que
ocurre después (estado, pagos, días añadidos) va en ``contrato_ejecucion``.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from secop.esquema import FUENTES_DE_RECURSOS
from secop.limpieza import (
    a_entero,
    a_fecha,
    a_numero,
    familia_unspsc,
    limpiar_categoria,
    limpiar_estado,
    segmento_unspsc,
    separar_duracion,
    separar_localizacion,
    texto_o_nulo,
    valor_positivo,
)


def _orden_actualizacion() -> F.Column:
    """Criterio para elegir la versión más reciente de un registro.

    Es un struct (fecha de actualización, año de carga, id): comparar structs
    compara campo por campo, así que gana la fila más reciente y, si empatan,
    el id decide. Que siempre gane la misma fila hace el resultado reproducible.
    """
    return F.struct(
        F.substring("ultima_actualizacion", 1, 19).try_cast("timestamp"),
        F.col("anio_carga"),
        F.col("id_contrato"),
    )


def contratos_unicos(bronce: DataFrame) -> DataFrame:
    """Una fila por id_contrato (la versión más reciente).

    En 2019 id_contrato es único, pero al cargar varios años un contrato podría
    repetirse; esta función lo garantiza sin depender de esa suposición.
    """
    con_orden = bronce.filter(texto_o_nulo("id_contrato").isNotNull()).withColumn(
        "_orden", _orden_actualizacion()
    )
    # max_by agrupa por id y se queda con la fila completa (struct) de mayor _orden.
    columnas = [c for c in con_orden.columns if c not in ("id_contrato", "_orden")]
    return (
        con_orden.groupBy("id_contrato")
        .agg(F.max_by(F.struct(*columnas), "_orden").alias("_fila"))
        .select("id_contrato", "_fila.*")
    )


def _vista_limpia(bronce: DataFrame) -> DataFrame:
    """Columnas ya limpias que usan las dimensiones y la revisión de dependencias.

    Centralizar aquí la limpieza garantiza que las dimensiones y la revisión de
    dependencias funcionales vean exactamente los mismos valores.
    """
    loc = separar_localizacion("localizaci_n")
    return (
        bronce.withColumns(loc)
        .select(
            "id_contrato",
            "anio_carga",
            "ultima_actualizacion",
            texto_o_nulo("codigo_entidad").alias("codigo_entidad"),
            texto_o_nulo("nombre_entidad").alias("nombre_entidad"),
            texto_o_nulo("nit_entidad").alias("nit_entidad"),
            limpiar_categoria("orden").alias("orden_entidad"),
            limpiar_categoria("sector").alias("sector_entidad"),
            limpiar_categoria("rama").alias("rama_entidad"),
            limpiar_categoria("entidad_centralizada").alias("entidad_centralizada"),
            # Si la columna departamento/ciudad viene vacía se usa la de localizaci_n.
            F.coalesce(texto_o_nulo("departamento"), F.col("departamento_loc")).alias(
                "departamento"
            ),
            F.coalesce(texto_o_nulo("ciudad"), F.col("ciudad_loc")).alias("municipio"),
            F.col("pais"),
            texto_o_nulo("proceso_de_compra").alias("proceso_de_compra"),
            limpiar_categoria("modalidad_de_contratacion").alias("modalidad_de_contratacion"),
            limpiar_categoria("justificacion_modalidad_de").alias("justificacion_modalidad"),
            texto_o_nulo("codigo_proveedor").alias("codigo_proveedor"),
            limpiar_categoria("tipodocproveedor").alias("tipo_doc_proveedor"),
            limpiar_categoria("es_grupo").alias("es_grupo"),
            texto_o_nulo("codigo_de_categoria_principal").alias("codigo_categoria"),
            familia_unspsc("codigo_de_categoria_principal").alias("familia_unspsc"),
            segmento_unspsc("codigo_de_categoria_principal").alias("segmento_unspsc"),
        )
        .withColumn("_orden", _orden_actualizacion())
    )


# ---------------------------------------------------------------------------
# Dimensiones
# ---------------------------------------------------------------------------
def construir_dim_departamento(bronce: DataFrame) -> DataFrame:
    """Una fila por departamento (PK: departamento), con su país.

    El país depende del departamento, no de la entidad: guardarlo en dim_entidad
    sería una dependencia transitiva (entidad -> departamento -> país).
    """
    return (
        _vista_limpia(bronce)
        .filter(F.col("departamento").isNotNull())
        .groupBy("departamento")
        .agg(F.max_by("pais", "_orden").alias("pais"))
    )


def construir_dim_municipio(bronce: DataFrame) -> DataFrame:
    """Una fila por municipio. PK compuesta: (departamento, municipio).

    ¿Por qué compuesta? Hay municipios con el mismo nombre en departamentos
    distintos (p. ej. Barbosa en Antioquia y en Santander): el nombre solo no
    identifica al municipio. ``departamento`` es además FK a dim_departamento.
    """
    return (
        _vista_limpia(bronce)
        .filter(F.col("departamento").isNotNull() & F.col("municipio").isNotNull())
        .select("departamento", "municipio")
        .distinct()
    )


def construir_dim_entidad(bronce: DataFrame) -> DataFrame:
    """Una fila por entidad compradora (PK: codigo_entidad).

    Problema observado: 8 códigos de entidad aparecen con dos nombres (nombre
    completo y sigla). Regla: se elige el nombre MÁS LARGO (el completo) y, si
    hay empate, el último en orden alfabético. Se guarda cuántos nombres tenía
    (``n_nombres``, dato de calidad que no se puede reconstruir de otra tabla).
    La ubicación se guarda SOLO como FK (departamento, municipio).
    """
    base = _vista_limpia(bronce).filter(F.col("codigo_entidad").isNotNull())
    atributos = F.struct(
        "nit_entidad",
        "orden_entidad",
        "sector_entidad",
        "rama_entidad",
        "entidad_centralizada",
        "departamento",
        "municipio",
    )
    return (
        base.groupBy("codigo_entidad")
        .agg(
            # max de un struct compara campo por campo: primero el largo del
            # nombre (gana el completo sobre la sigla) y, si empatan, el texto
            # (desempate alfabético). Siempre da el mismo resultado.
            F.max(F.struct(F.length("nombre_entidad").alias("largo"), F.col("nombre_entidad")))[
                "nombre_entidad"
            ].alias("nombre_entidad"),
            F.countDistinct("nombre_entidad").alias("n_nombres"),
            F.max_by(atributos, "_orden").alias("_attr"),
        )
        .select("codigo_entidad", "nombre_entidad", "n_nombres", "_attr.*")
    )


def construir_dim_proveedor(bronce: DataFrame) -> DataFrame:
    """Una fila por proveedor (PK: codigo_proveedor), SIN datos personales.

    Solo se guarda el código interno de SECOP (no es la cédula ni el NIT), el
    TIPO de documento (persona natural o jurídica, no identifica a nadie) y si
    es un grupo (consorcio o unión temporal), que es una característica estable
    del proveedor. El nombre y el número de documento no existen desde bronce.
    """
    return (
        _vista_limpia(bronce)
        .filter(F.col("codigo_proveedor").isNotNull())
        .groupBy("codigo_proveedor")
        .agg(
            F.max_by("tipo_doc_proveedor", "_orden").alias("tipo_doc_proveedor"),
            F.max_by("es_grupo", "_orden").alias("es_grupo"),
        )
    )


def construir_dim_familia(bronce: DataFrame) -> DataFrame:
    """Una fila por familia UNSPSC (PK: familia_unspsc), con su segmento.

    Jerarquía UNSPSC: segmento (80) > familia (8011) > ... > código (V1.80111600).
    El segmento depende de la familia, no del código: por eso va aquí y no en
    dim_categoria (evita la dependencia transitiva código -> familia -> segmento).
    """
    return (
        _vista_limpia(bronce)
        .filter(F.col("familia_unspsc").isNotNull())
        .select("familia_unspsc", "segmento_unspsc")
        .distinct()
    )


def construir_dim_categoria(bronce: DataFrame) -> DataFrame:
    """Una fila por código de categoría (PK: codigo_categoria), con FK a su familia."""
    return (
        _vista_limpia(bronce)
        .filter(F.col("codigo_categoria").isNotNull())
        .select("codigo_categoria", "familia_unspsc")
        .distinct()
    )


def construir_dim_proceso(bronce: DataFrame) -> DataFrame:
    """Una fila por proceso de compra (PK: proceso_de_compra).

    En SECOP un proceso de compra puede generar varios contratos, y la modalidad
    (licitación, contratación directa...) y su justificación son del PROCESO.
    Guardarlas en cada contrato sería una dependencia transitiva
    (contrato -> proceso -> modalidad). ``n_modalidades`` registra cuántas
    modalidades distintas tenía el proceso en la fuente: debe ser 1; si no lo es,
    la dependencia no se cumple en los datos (ver ``revisar_dependencias``).
    """
    return (
        _vista_limpia(bronce)
        .filter(F.col("proceso_de_compra").isNotNull())
        .groupBy("proceso_de_compra")
        .agg(
            F.max_by("modalidad_de_contratacion", "_orden").alias("modalidad_de_contratacion"),
            F.max_by("justificacion_modalidad", "_orden").alias("justificacion_modalidad"),
            F.countDistinct("modalidad_de_contratacion").alias("n_modalidades"),
        )
    )


# ---------------------------------------------------------------------------
# Hechos
# ---------------------------------------------------------------------------
def construir_contrato(bronce: DataFrame) -> DataFrame:
    """Tabla de contratos con la información disponible AL FIRMAR.

    Solo atributos que dependen del contrato (y no de la entidad, el proveedor,
    la categoría o el proceso, que están en sus dimensiones).
    ``es_pyme`` se queda aquí a propósito: una empresa puede ser pyme en 2019 y
    dejar de serlo en 2020, así que el valor depende del contrato (del momento
    de la firma), no solo del proveedor.
    ``fecha_fin`` y la duración se guardan, pero OJO: no está confirmado si ya
    incluyen los días añadidos. Por eso el modelo principal no las usa.
    """
    unicos = contratos_unicos(bronce)
    duracion = separar_duracion("duraci_n_del_contrato")
    return unicos.withColumns(duracion).select(
        "id_contrato",
        # Llaves foráneas hacia las dimensiones
        texto_o_nulo("codigo_entidad").alias("codigo_entidad"),
        texto_o_nulo("codigo_proveedor").alias("codigo_proveedor"),
        texto_o_nulo("codigo_de_categoria_principal").alias("codigo_categoria"),
        texto_o_nulo("proceso_de_compra").alias("proceso_de_compra"),
        # Identificador propio del contrato (no personal)
        texto_o_nulo("referencia_del_contrato").alias("referencia_del_contrato"),
        # Características del contrato
        limpiar_categoria("tipo_de_contrato").alias("tipo_de_contrato"),
        limpiar_categoria("condiciones_de_entrega").alias("condiciones_de_entrega"),
        limpiar_categoria("es_pyme").alias("es_pyme"),
        limpiar_categoria("habilita_pago_adelantado").alias("habilita_pago_adelantado"),
        limpiar_categoria("liquidaci_n").alias("requiere_liquidacion"),
        limpiar_categoria("obligaci_n_ambiental").alias("obligacion_ambiental"),
        limpiar_categoria("obligaciones_postconsumo").alias("obligaciones_postconsumo"),
        limpiar_categoria("reversion").alias("reversion"),
        limpiar_categoria("origen_de_los_recursos").alias("origen_de_los_recursos"),
        limpiar_categoria("destino_gasto").alias("destino_gasto"),
        limpiar_categoria("espostconflicto").alias("es_postconflicto"),
        limpiar_categoria("el_contrato_puede_ser_prorrogado").alias("puede_ser_prorrogado"),
        # Fechas (texto AAAA-MM-DD... -> date)
        a_fecha("fecha_de_firma").alias("fecha_firma"),
        a_fecha("fecha_de_inicio_del_contrato").alias("fecha_inicio"),
        a_fecha("fecha_de_fin_del_contrato").alias("fecha_fin"),
        # Valores: <= 0 -> NULL. El recorte en el tope se aplica en oro.
        valor_positivo("valor_del_contrato").alias("valor_contrato"),
        a_numero("valor_de_pago_adelantado").alias("valor_pago_adelantado"),
        # Duración declarada en valores atómicos (los días se calculan en oro)
        "duracion_numero",
        "duracion_unidad",
        "anio_carga",
    )


def construir_fuente_recurso(bronce: DataFrame) -> DataFrame:
    """Pasa las 6 columnas de fuentes de recursos de formato ancho a largo (1FN).

    Antes (1 fila por contrato, 6 columnas = grupo repetido, viola 1FN):
        id | pgn | sgp | regalias | ...
    Después (1 fila por contrato y fuente con valor > 0):
        id | fuente | valor
    PK compuesta (id_contrato, fuente). 2FN: ``valor`` depende de las DOS partes
    de la llave (el aporte de ESA fuente a ESE contrato).
    ``stack(n, etiqueta1, col1, ...)`` genera n filas por cada fila de entrada.
    """
    pares = ", ".join(f"'{corto}', `{crudo}`" for corto, crudo in FUENTES_DE_RECURSOS.items())
    expresion = f"stack({len(FUENTES_DE_RECURSOS)}, {pares}) as (fuente, valor_texto)"
    return (
        contratos_unicos(bronce)
        .select("id_contrato", F.expr(expresion))
        .withColumn("valor", a_numero("valor_texto"))
        .filter(F.col("valor") > 0)  # solo las fuentes que realmente aportan dinero
        .select("id_contrato", "fuente", "valor")
    )


def construir_ejecucion(bronce: DataFrame) -> DataFrame:
    """Información POSTERIOR a la firma (relación 1 a 1 con contrato).

    Nunca entra como variable al modelo. ``dias_adicionados`` se usa solo para
    construir la etiqueta en oro.
    """
    return contratos_unicos(bronce).select(
        "id_contrato",
        limpiar_estado("estado_contrato").alias("estado_contrato"),
        a_numero("valor_facturado").alias("valor_facturado"),
        a_numero("valor_pendiente_de_pago").alias("valor_pendiente_de_pago"),
        a_numero("valor_pagado").alias("valor_pagado"),
        a_numero("valor_amortizado").alias("valor_amortizado"),
        a_numero("valor_pendiente_de").alias("valor_pendiente_amortizacion"),
        a_numero("valor_pendiente_de_ejecucion").alias("valor_pendiente_de_ejecucion"),
        a_numero("saldo_cdp").alias("saldo_cdp"),
        a_numero("saldo_vigencia").alias("saldo_vigencia"),
        a_entero("dias_adicionados").alias("dias_adicionados"),
        a_fecha("fecha_inicio_liquidacion").alias("fecha_inicio_liquidacion"),
        a_fecha("fecha_fin_liquidacion").alias("fecha_fin_liquidacion"),
        a_fecha("fecha_de_notificaci_n_de_prorrogaci_n").alias("fecha_notificacion_prorroga"),
        F.substring("ultima_actualizacion", 1, 19)
        .try_cast("timestamp")
        .alias("ultima_actualizacion"),
    )


# ---------------------------------------------------------------------------
# Verificaciones del modelo relacional
# ---------------------------------------------------------------------------
# Dependencias funcionales en las que se apoya la normalización:
# (nombre, columnas que determinan, columna determinada).
# "X -> Y" significa: cada valor de X tiene UN solo valor de Y.
DEPENDENCIAS_FUNCIONALES: list[tuple[str, list[str], str]] = [
    ("entidad -> nombre", ["codigo_entidad"], "nombre_entidad"),
    ("departamento -> pais", ["departamento"], "pais"),
    ("proceso -> modalidad", ["proceso_de_compra"], "modalidad_de_contratacion"),
    ("proceso -> justificacion", ["proceso_de_compra"], "justificacion_modalidad"),
    ("proveedor -> tipo de documento", ["codigo_proveedor"], "tipo_doc_proveedor"),
    ("proveedor -> es_grupo", ["codigo_proveedor"], "es_grupo"),
    ("categoria -> familia", ["codigo_categoria"], "familia_unspsc"),
    ("familia -> segmento", ["familia_unspsc"], "segmento_unspsc"),
]


def revisar_dependencias(bronce: DataFrame) -> DataFrame:
    """Comprueba en los DATOS las dependencias funcionales que asume el modelo.

    Para cada dependencia X -> Y cuenta cuántos valores de X aparecen con más de
    un valor de Y (violaciones). Lo ideal es 0. Si no es 0, la dimensión
    resuelve el conflicto con una regla explícita (nombre más largo, valor más
    reciente) y el número queda documentado en docs/informe.md.
    Ejemplo esperado: en 2019, "entidad -> nombre" tiene 8 violaciones
    (entidades con nombre completo y sigla).
    """
    vista = _vista_limpia(bronce)
    partes = []
    for nombre, determinantes, determinado in DEPENDENCIAS_FUNCIONALES:
        no_nulos = F.lit(True)
        for c in determinantes + [determinado]:
            no_nulos = no_nulos & F.col(c).isNotNull()
        partes.append(
            vista.filter(no_nulos)
            .groupBy(*determinantes)
            .agg(F.countDistinct(determinado).alias("_n"))
            .agg(
                F.count("*").alias("valores_determinante"),
                F.coalesce(F.sum((F.col("_n") > 1).cast("long")), F.lit(0)).alias("violaciones"),
            )
            .select(F.lit(nombre).alias("dependencia"), "valores_determinante", "violaciones")
        )
    resultado = partes[0]
    for parte in partes[1:]:
        resultado = resultado.unionByName(parte)
    return resultado


def filas_huerfanas(hijo: DataFrame, padre: DataFrame, llave: str | list[str]) -> DataFrame:
    """Filas de ``hijo`` cuya llave (simple o compuesta) no existe en ``padre``.

    left_anti devuelve las filas de la izquierda SIN pareja a la derecha.
    Si el modelo está bien construido, el conteo debe ser 0. Las filas con
    alguna parte de la llave en NULL se excluyen: un contrato sin proveedor
    registrado no es una referencia rota.
    """
    llaves = [llave] if isinstance(llave, str) else list(llave)
    completas = F.lit(True)
    for c in llaves:
        completas = completas & F.col(c).isNotNull()
    return hijo.filter(completas).join(padre.select(*llaves), on=llaves, how="left_anti")


# Relaciones FK -> PK del modelo: (tabla hija, tabla padre, llave).
# Se usa en el notebook 03 y en los tests para verificar integridad referencial.
RELACIONES: list[tuple[str, str, str | list[str]]] = [
    ("dim_municipio", "dim_departamento", "departamento"),
    ("dim_entidad", "dim_municipio", ["departamento", "municipio"]),
    ("dim_entidad", "dim_departamento", "departamento"),
    ("dim_categoria", "dim_familia_unspsc", "familia_unspsc"),
    ("contrato", "dim_entidad", "codigo_entidad"),
    ("contrato", "dim_proveedor", "codigo_proveedor"),
    ("contrato", "dim_categoria", "codigo_categoria"),
    ("contrato", "dim_proceso", "proceso_de_compra"),
    ("contrato_fuente_recurso", "contrato", "id_contrato"),
    ("contrato_ejecucion", "contrato", "id_contrato"),
]

# Llave primaria de cada tabla (para verificar unicidad).
LLAVES_PRIMARIAS: dict[str, list[str]] = {
    "dim_departamento": ["departamento"],
    "dim_municipio": ["departamento", "municipio"],
    "dim_entidad": ["codigo_entidad"],
    "dim_proveedor": ["codigo_proveedor"],
    "dim_familia_unspsc": ["familia_unspsc"],
    "dim_categoria": ["codigo_categoria"],
    "dim_proceso": ["proceso_de_compra"],
    "contrato": ["id_contrato"],
    "contrato_fuente_recurso": ["id_contrato", "fuente"],
    "contrato_ejecucion": ["id_contrato"],
}

# Funciones que construyen cada tabla, en orden de dependencia (padres primero).
CONSTRUCTORES = {
    "dim_departamento": construir_dim_departamento,
    "dim_municipio": construir_dim_municipio,
    "dim_entidad": construir_dim_entidad,
    "dim_proveedor": construir_dim_proveedor,
    "dim_familia_unspsc": construir_dim_familia,
    "dim_categoria": construir_dim_categoria,
    "dim_proceso": construir_dim_proceso,
    "contrato": construir_contrato,
    "contrato_fuente_recurso": construir_fuente_recurso,
    "contrato_ejecucion": construir_ejecucion,
}


def construir_plata(bronce: DataFrame) -> dict[str, DataFrame]:
    """Construye las 10 tablas de plata. Devuelve {nombre_corto: DataFrame}."""
    return {nombre: funcion(bronce) for nombre, funcion in CONSTRUCTORES.items()}
