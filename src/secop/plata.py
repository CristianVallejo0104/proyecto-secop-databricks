"""Capa plata: modelo normalizado (tablas limpias y relacionadas).

Tablas que se construyen a partir de bronce:

    dim_entidad (codigo_entidad)  ─┐
    dim_proveedor (codigo_proveedor)├─< contrato (id_contrato) >── contrato_fuente_recurso
    dim_categoria (codigo_categoria)┘           │
                                                └── contrato_ejecucion (1 a 1)

Idea central: ``contrato`` tiene SOLO lo que se conoce al firmar; todo lo que
ocurre después (estado, pagos, saldos, días añadidos) va en
``contrato_ejecucion``. Así es imposible usar por descuido una variable
posterior a la firma: tendría que unirse explícitamente.
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


# ---------------------------------------------------------------------------
# Dimensiones
# ---------------------------------------------------------------------------
def construir_dim_entidad(bronce: DataFrame) -> DataFrame:
    """Una fila por entidad compradora (llave: codigo_entidad).

    Problema observado: 8 códigos de entidad aparecen con dos nombres (nombre
    completo y sigla). Regla: se elige el nombre MÁS LARGO (el completo) y, si
    hay empate, el último en orden alfabético. Se guarda cuántos nombres tenía.
    Los demás atributos se toman de la fila más reciente.
    """
    loc = separar_localizacion("localizaci_n")
    base = (
        bronce.withColumn("codigo_entidad", texto_o_nulo("codigo_entidad"))
        .filter(F.col("codigo_entidad").isNotNull())
        .withColumn("nombre_limpio", texto_o_nulo("nombre_entidad"))
        .withColumn("_orden", _orden_actualizacion())
        .withColumns(loc)
    )
    atributos = F.struct(
        texto_o_nulo("nit_entidad").alias("nit_entidad"),
        limpiar_categoria("orden").alias("orden_entidad"),
        limpiar_categoria("sector").alias("sector_entidad"),
        limpiar_categoria("rama").alias("rama_entidad"),
        limpiar_categoria("entidad_centralizada").alias("entidad_centralizada"),
        # Si la columna departamento viene vacía se usa la de localizaci_n.
        F.coalesce(texto_o_nulo("departamento"), F.col("departamento_loc")).alias(
            "departamento_entidad"
        ),
        F.coalesce(texto_o_nulo("ciudad"), F.col("ciudad_loc")).alias("ciudad_entidad"),
        F.col("pais").alias("pais_entidad"),
    )
    return (
        base.groupBy("codigo_entidad")
        .agg(
            # max de un struct compara campo por campo: primero el largo del
            # nombre (gana el completo sobre la sigla) y, si empatan, el texto
            # (desempate alfabético). Siempre da el mismo resultado.
            F.max(F.struct(F.length("nombre_limpio").alias("largo"), F.col("nombre_limpio")))[
                "nombre_limpio"
            ].alias("nombre_entidad"),
            F.countDistinct("nombre_limpio").alias("n_nombres"),
            F.max_by(atributos, "_orden").alias("_attr"),
        )
        .select("codigo_entidad", "nombre_entidad", "n_nombres", "_attr.*")
    )


def construir_dim_proveedor(bronce: DataFrame) -> DataFrame:
    """Una fila por proveedor (llave: codigo_proveedor), SIN datos personales.

    Solo se guarda el código interno de SECOP (no es la cédula ni el NIT), el
    TIPO de documento (indica persona natural o jurídica, no identifica a nadie)
    y la fecha de su primer contrato firmado en los datos cargados.
    El nombre y el número de documento del proveedor ya no existen desde bronce.
    """
    return (
        bronce.withColumn("codigo_proveedor", texto_o_nulo("codigo_proveedor"))
        .filter(F.col("codigo_proveedor").isNotNull())
        .withColumn("_orden", _orden_actualizacion())
        .groupBy("codigo_proveedor")
        .agg(
            F.max_by(limpiar_categoria("tipodocproveedor"), "_orden").alias("tipo_doc_proveedor"),
            F.min(a_fecha("fecha_de_firma")).alias("fecha_primer_contrato"),
        )
    )


def construir_dim_categoria(bronce: DataFrame) -> DataFrame:
    """Una fila por código de categoría UNSPSC, con su familia y segmento."""
    return (
        bronce.select(texto_o_nulo("codigo_de_categoria_principal").alias("codigo_categoria"))
        .filter(F.col("codigo_categoria").isNotNull())
        .distinct()
        .withColumn("familia_unspsc", familia_unspsc("codigo_categoria"))
        .withColumn("segmento_unspsc", segmento_unspsc("codigo_categoria"))
    )


# ---------------------------------------------------------------------------
# Hechos
# ---------------------------------------------------------------------------
def construir_contrato(bronce: DataFrame) -> DataFrame:
    """Tabla de contratos con la información disponible AL FIRMAR.

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
        # Identificadores del proceso (no personales)
        texto_o_nulo("proceso_de_compra").alias("proceso_de_compra"),
        texto_o_nulo("referencia_del_contrato").alias("referencia_del_contrato"),
        # Características del contrato
        limpiar_categoria("tipo_de_contrato").alias("tipo_de_contrato"),
        limpiar_categoria("modalidad_de_contratacion").alias("modalidad_de_contratacion"),
        limpiar_categoria("justificacion_modalidad_de").alias("justificacion_modalidad"),
        limpiar_categoria("condiciones_de_entrega").alias("condiciones_de_entrega"),
        limpiar_categoria("es_grupo").alias("es_grupo"),
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
        # Duración declarada separada en número, unidad y días aproximados
        "duracion_numero",
        "duracion_unidad",
        "duracion_dias_declarada",
        "anio_carga",
    )


def construir_fuente_recurso(bronce: DataFrame) -> DataFrame:
    """Pasa las 6 columnas de fuentes de recursos de formato ancho a largo.

    Antes (1 fila por contrato, 6 columnas):
        id | pgn | sgp | regalias | ...
    Después (1 fila por contrato y fuente con valor > 0):
        id | fuente | valor
    ``stack(n, etiqueta1, col1, ...)`` genera n filas por cada fila de entrada.
    Formato largo = tabla normalizada: agregar una fuente nueva no cambia el esquema.
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
# Integridad referencial
# ---------------------------------------------------------------------------
def filas_huerfanas(hijo: DataFrame, padre: DataFrame, llave: str) -> DataFrame:
    """Filas de ``hijo`` cuya llave no existe en ``padre``.

    left_anti devuelve las filas de la izquierda SIN pareja a la derecha.
    Si el modelo está bien construido, el conteo debe ser 0. Las llaves NULL se
    excluyen: un contrato sin proveedor registrado no es una referencia rota.
    """
    return hijo.filter(F.col(llave).isNotNull()).join(
        padre.select(llave), on=llave, how="left_anti"
    )
