"""Esquema de la fuente y clasificación de columnas.

Este es el ÚNICO archivo del repositorio que escribe los nombres de las columnas
con datos personales. El resto del código los usa a través de las constantes de
aquí; así, para auditar el proyecto basta con revisar este archivo, y un test
(tests/test_privacidad.py) comprueba que esos nombres no aparezcan en otro lado.

Si el profesor pide cambiar de base de datos, lo que cambia es este archivo
(y ``config.py``): el resto del código no conoce nombres de columnas crudas.
"""

# ---------------------------------------------------------------------------
# 1. Las 95 columnas del conjunto "SECOP II - Contratos Electrónicos" (jbjy-vk9h),
#    en el mismo orden en que las entrega la API de Socrata.
# ---------------------------------------------------------------------------
COLUMNAS_CRUDAS: list[str] = [
    "nombre_entidad",
    "nit_entidad",
    "departamento",
    "ciudad",
    "localizaci_n",
    "orden",
    "sector",
    "rama",
    "entidad_centralizada",
    "proceso_de_compra",
    "id_contrato",
    "referencia_del_contrato",
    "estado_contrato",
    "codigo_de_categoria_principal",
    "descripcion_del_proceso",
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "justificacion_modalidad_de",
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "condiciones_de_entrega",
    "tipodocproveedor",
    "documento_proveedor",
    "proveedor_adjudicado",
    "es_grupo",
    "es_pyme",
    "habilita_pago_adelantado",
    "liquidaci_n",
    "obligaci_n_ambiental",
    "obligaciones_postconsumo",
    "reversion",
    "origen_de_los_recursos",
    "destino_gasto",
    "valor_del_contrato",
    "valor_de_pago_adelantado",
    "valor_facturado",
    "valor_pendiente_de_pago",
    "valor_pagado",
    "valor_amortizado",
    "valor_pendiente_de",
    "valor_pendiente_de_ejecucion",
    "saldo_cdp",
    "saldo_vigencia",
    "espostconflicto",
    "dias_adicionados",
    "puntos_del_acuerdo",
    "pilares_del_acuerdo",
    "urlproceso",
    "nombre_representante_legal",
    "nacionalidad_representante_legal",
    "domicilio_representante_legal",
    "tipo_de_identificaci_n_representante_legal",
    "identificaci_n_representante_legal",
    "g_nero_representante_legal",
    "presupuesto_general_de_la_nacion_pgn",
    "sistema_general_de_participaciones",
    "sistema_general_de_regal_as",
    "recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_",
    "recursos_de_credito",
    "recursos_propios",
    "ultima_actualizacion",
    "codigo_entidad",
    "codigo_proveedor",
    "fecha_inicio_liquidacion",
    "fecha_fin_liquidacion",
    "objeto_del_contrato",
    "duraci_n_del_contrato",
    "nombre_del_banco",
    "tipo_de_cuenta",
    "n_mero_de_cuenta",
    "el_contrato_puede_ser_prorrogado",
    "fecha_de_notificaci_n_de_prorrogaci_n",
    "nombre_ordenador_del_gasto",
    "tipo_de_documento_ordenador_del_gasto",
    "n_mero_de_documento_ordenador_del_gasto",
    "nombre_supervisor",
    "tipo_de_documento_supervisor",
    "n_mero_de_documento_supervisor",
    "nombre_ordenador_de_pago",
    "tipo_de_documento_ordenador_de_pago",
    "n_mero_de_documento_ordenador_de_pago",
    "documentos_tipo",
    "descripcion_documentos_tipo",
    "direcci_n_de_ejecuci_n_del_contrato",
    "criterios_de_sostenibilidad_ambiental",
    "requisitos_ambientales_en_las_especificaciones_t_cnicas",
    "criterios_ambientales_en_la_evaluaci_n_de_las_ofertas",
    "obligaciones_contractuales_o_poscontractuales_a_nivel_ambiental",
    "uso_de_etiquetado_ecol_gico",
    "obligaciones_posconsumo",
    "fecha_inicio_obligaciones_posconsumo",
    "fecha_fin_obligaciones_posconsumo",
    "fecha_inicio_reversi_n",
    "fecha_fin_reversi_n",
]

# ---------------------------------------------------------------------------
# 2. Datos personales: se eliminan ANTES de escribir la primera tabla (bronce).
#    Incluye nombres, documentos, género, domicilio y datos bancarios de
#    personas naturales (representante legal, supervisor, ordenadores) y el
#    nombre y documento del proveedor. La nacionalidad del representante también
#    se excluye por prudencia: es un atributo de una persona natural.
# ---------------------------------------------------------------------------
COLUMNAS_PERSONALES: list[str] = [
    # Representante legal
    "nombre_representante_legal",
    "nacionalidad_representante_legal",
    "domicilio_representante_legal",
    "tipo_de_identificaci_n_representante_legal",
    "identificaci_n_representante_legal",
    "g_nero_representante_legal",
    # Datos bancarios
    "nombre_del_banco",
    "tipo_de_cuenta",
    "n_mero_de_cuenta",
    # Ordenador del gasto, supervisor y ordenador de pago (nombre y documentos)
    "nombre_ordenador_del_gasto",
    "tipo_de_documento_ordenador_del_gasto",
    "n_mero_de_documento_ordenador_del_gasto",
    "nombre_supervisor",
    "tipo_de_documento_supervisor",
    "n_mero_de_documento_supervisor",
    "nombre_ordenador_de_pago",
    "tipo_de_documento_ordenador_de_pago",
    "n_mero_de_documento_ordenador_de_pago",
    # Proveedor: nombre y número de documento
    "proveedor_adjudicado",
    "documento_proveedor",
]

# Columnas que sí van a bronce: todas menos las personales, en el orden original.
COLUMNAS_BRONCE: list[str] = [c for c in COLUMNAS_CRUDAS if c not in COLUMNAS_PERSONALES]

# ---------------------------------------------------------------------------
# 3. Información POSTERIOR a la firma (fuga de información si entra al modelo).
#    Se guarda en plata (contrato_ejecucion) para análisis, pero nunca en las
#    variables de oro. "dias_adicionados" solo se usa para construir la etiqueta.
# ---------------------------------------------------------------------------
COLUMNAS_POSTERIORES_FIRMA: list[str] = [
    "estado_contrato",
    "valor_facturado",
    "valor_pendiente_de_pago",
    "valor_pagado",
    "valor_amortizado",
    "valor_pendiente_de",
    "valor_pendiente_de_ejecucion",
    "saldo_cdp",
    "saldo_vigencia",
    "dias_adicionados",
    "fecha_inicio_liquidacion",
    "fecha_fin_liquidacion",
    "fecha_de_notificaci_n_de_prorrogaci_n",
    "ultima_actualizacion",
]

# Columna cruda que origina la etiqueta.
COLUMNA_ETIQUETA_CRUDA = "dias_adicionados"

# ---------------------------------------------------------------------------
# 4. Fuentes de recursos (6 columnas "anchas") que en plata se apilan en filas.
#    Clave: nombre corto de la fuente; valor: columna cruda.
# ---------------------------------------------------------------------------
FUENTES_DE_RECURSOS: dict[str, str] = {
    "pgn": "presupuesto_general_de_la_nacion_pgn",
    "sgp": "sistema_general_de_participaciones",
    "regalias": "sistema_general_de_regal_as",
    "propios_territoriales": "recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_",
    "credito": "recursos_de_credito",
    "propios": "recursos_propios",
}
