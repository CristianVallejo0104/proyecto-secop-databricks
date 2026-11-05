# Informe: predicción de prórrogas en contratos SECOP II

**Autores:** Cristian (@CristianVallejo0104) y JP (@Juanxct) · Computación en la Nube · USTA Bogotá

> Estado: **resultados PENDIENTES**. Este informe se completa después de ejecutar
> los notebooks en Databricks. Ninguna cifra de resultados se escribe antes de
> obtenerla. Las cifras de "referencia" vienen de la exploración previa de 2019.

## 1. Introducción

Una prórroga (días añadidos al plazo) indica que el contrato no se cumplió en el
tiempo planeado. Anticiparla al firmar permitiría a las entidades planear mejor y
a los veedores priorizar seguimientos.

**Pregunta:** con la información disponible al firmar un contrato de SECOP II,
¿se le añadirán días después?

**Etiqueta:** `dias_adicionados > 0`. Referencia 2019: ~11 % de positivos.

## 2. Datos

- Fuente: "SECOP II - Contratos Electrónicos" (datos.gov.co, id `jbjy-vk9h`),
  ~5,67 millones de filas (2015-2026), 95 columnas.
- Años cargados: 2019 (142.090 filas) y 2020 (356.623 filas).
- Excluidos del entrenamiento: 2025-2026 (etiqueta incompleta).
- Datos personales: eliminados antes de la primera tabla (ver `docs/decisiones.md` §4).

| Verificación | Referencia | Obtenido |
|---|---|---|
| Filas bronce 2019 | 142.090 | PENDIENTE |
| Filas bronce 2020 | 356.623 | PENDIENTE |
| Huérfanas en integridad referencial | 0 | PENDIENTE |
| Filas oro 2019 | 141.257 | PENDIENTE |
| Tasa de prórroga oro 2019 | 10,99 % | PENDIENTE |
| Filas oro 2020 | — | PENDIENTE |
| Tasa de prórroga oro 2020 | — | PENDIENTE |

## 3. Metodología

1. **Ingesta** por año y mes con reintentos y reanudación (notebook 01).
2. **Bronce** Delta, texto, particionado por año de carga (02).
3. **Plata** normalizada: 3 dimensiones, contrato, fuentes de recursos y ejecución (03).
4. **Oro** con etiqueta y 28 variables conocidas al firmar, más 2 de plazo aparte (04).
5. **Descriptivo** de tasas por grupo (05).
6. **Modelo** (06): partición temporal (entrenamiento < 2020-04-01 ≤ validación <
   2020-07-01 ≤ prueba), pesos de clase, regresión logística (línea base) y GBT,
   umbral por máximo F1 en validación, tres variantes para detectar fuga en el plazo,
   registro en MLflow.

## 4. Resultados descriptivos

| Agrupación | Hallazgo principal |
|---|---|
| Tasa global (años completos) | PENDIENTE |
| Por año y mes de firma | PENDIENTE |
| Por tipo de contrato | PENDIENTE |
| Por modalidad | PENDIENTE |
| Por departamento | PENDIENTE |
| Por entidad (≥ 100 contratos) | PENDIENTE |
| Cláusula "puede ser prorrogado" | PENDIENTE |
| Diagnóstico de plazo por clase (§6 del notebook 05) | PENDIENTE |

## 5. Resultados del modelo (partición de prueba)

Motor usado: PENDIENTE (MLlib o scikit-learn; si fue el plan B, anotar el motivo registrado en MLflow).

| Variante | Algoritmo | AUC-ROC | AUC-PR | F1 | Precisión | Sensibilidad | Exactitud | Umbral |
|---|---|---|---|---|---|---|---|---|
| sin_plazo | logística | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE |
| sin_plazo | árboles | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE |
| con_duracion | logística | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE |
| con_duracion | árboles | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE |
| con_plazo | logística | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE |
| con_plazo | árboles | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE | PENDIENTE |

Referencia sin información: AUC-ROC = 0,5; AUC-PR = tasa de positivos en prueba = PENDIENTE.

## 6. Prueba de fuga en el plazo

| Algoritmo | ΔAUC con_duracion − sin_plazo | ΔAUC con_plazo − sin_plazo | ¿Sospechoso (> 0,05)? |
|---|---|---|---|
| logística | PENDIENTE | PENDIENTE | PENDIENTE |
| árboles | PENDIENTE | PENDIENTE | PENDIENTE |

Interpretación: PENDIENTE. Modelo final elegido: PENDIENTE (por defecto `sin_plazo`
si hay salto sospechoso).

## 7. Discusión

PENDIENTE. Puntos a cubrir:
- ¿Supera el modelo a la referencia sin información en AUC-PR? ¿Por cuánto?
- ¿Qué variables o grupos explican más las prórrogas (según el descriptivo)?
- ¿Mejoran los árboles a la línea base lo suficiente para justificar su complejidad?
- Efecto posible de la pandemia (la prueba es el segundo semestre de 2020).

## 8. Limitaciones

- Solo dos años cargados; 2020 es atípico por la pandemia.
- No está confirmado si la fecha de fin incluye los días añadidos (de ahí la prueba de fuga).
- Contratos sin fecha de firma no se descargan (el filtro de la API es por fecha de firma).
- La etiqueta solo mide si hubo días añadidos, no cuántos ni por qué.
- La evaluación del plan B (si se usa) es sobre una muestra, no sobre toda la prueba.

## 9. Conclusiones

PENDIENTE.

## 10. Reproducibilidad

| Elemento | Dónde |
|---|---|
| Versión del código | `src/secop/__init__.py` (`__version__`), etiqueta de git y run de MLflow |
| Dependencias | `requirements*.txt` con `==` |
| Semilla | `config.SEMILLA = 42` |
| Tablas | Delta con historial (`DESCRIBE HISTORY`) |
| Experimento | MLflow `/Users/<usuario>/secop_prorrogas` |
| Pruebas | `make test`, `make simular`, GitHub Actions |
