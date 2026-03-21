# Architecture Decision Records (ADR)

Registro de decisiones arquitectónicas clave, motivaciones, alternativas evaluadas y tradeoffs.

---

## ADR-001 - Medallion en Filesystem (Landing → Bronze → Quarantine)

**Decisión:** Separar filesystem por capas (`landing/` → `bronze/` → `quarantine/`).

**Motivo:**  
Evitar procesamiento de archivos parciales, permitir re-proceso controlado y auditar input original.

**Alternativas consideradas:**
1. Procesar directamente desde `landing/`.
2. No separar capas en filesystem.
3. Usar únicamente base de datos sin capa intermedia.

**Motivo de descarte:**
- Riesgo de procesar archivos incompletos.
- Menor trazabilidad.
- Dificultad para auditoría del input original.

**Tradeoffs:**  
Mayor complejidad operativa en carpetas, a cambio de mayor control y robustez.

---

## ADR-002 - RAW en PostgreSQL (`schema raw`)

**Decisión:** Persistir la ingesta en PostgreSQL dentro del esquema `raw`.

**Motivo:**  
Consultabilidad vía SQL, atomicidad transaccional e integración directa con dbt.

**Alternativas consideradas:**
1. Mantener datos solo en filesystem (data lake simple).
2. Transformar directamente en memoria (Pandas) sin persistencia intermedia.
3. Insertar directamente en tablas finales.

**Motivo de descarte:**
- Filesystem no permite auditoría estructurada.
- Transformaciones en memoria reducen trazabilidad.
- Insertar directo en capa final rompe separación de responsabilidades.

**Tradeoffs:**  
Mayor uso de almacenamiento, a cambio de integridad y gobernanza.

---

## ADR-003 - Quarantine (No descartar inválidos)

**Decisión:** Enviar registros inválidos a tablas `_quarantine` y conservar evidencia en filesystem.

**Motivo:**  
Auditoría, calidad de datos y prevención de pérdida silenciosa.

**Alternativas consideradas:**
1. Descartar registros inválidos.
2. Mezclar válidos e inválidos con flag.
3. Solo loggear el error sin persistir datos.

**Motivo de descarte:**
- Descartar implica pérdida irreversible.
- Mezclar complica consumo analítico.
- Solo logs no permiten reprocesamiento ni auditoría estructurada.

**Tradeoffs:**  
Mayor gestión de tablas y almacenamiento, a cambio de trazabilidad completa.

---

## ADR-004 - Control de Batches (`raw.batch_control`)

**Decisión:** Implementar control plane en base de datos mediante `raw.batch_control`.

**Motivo:**  
Idempotencia, reprocesamiento seguro, conteos por batch y estado consultable vía SQL.

**Alternativas consideradas:**
1. Confiar solo en logs.
2. Delegar control únicamente al orquestador (Prefect).
3. No registrar metadata por ejecución.

**Motivo de descarte:**
- Logs no son fácilmente consultables.
- Orquestador no sustituye control de datos.
- Sin metadata no hay auditoría real.

**Tradeoffs:**  
Mayor disciplina operativa, a cambio de control estructurado y auditabilidad.

---

## ADR-005 - dbt para Transformaciones y Modelo Dimensional

**Decisión:** Utilizar `dbt build` para construir staging lógico y modelo analítico (fact + dims + marts).

**Motivo:**  
SQL versionado, tests integrados, reproducibilidad y linaje claro.

**Alternativas consideradas:**
1. SQL manual dentro de scripts Python.
2. Transformaciones en Pandas.
3. Modelo plano sin esquema dimensional.

**Motivo de descarte:**
- Scripts acoplan lógica y transformación.
- Pandas no escala bien para modelo analítico persistente.
- Modelo plano limita performance y claridad semántica.

**Tradeoffs:**  
Requiere disciplina en models y profiles, pero mejora mantenibilidad.

---

## ADR-006 - `public` como capa Gold (tradeoff intencional)

**Decisión:** Mantener modelo dimensional y marts en `public`.

**Motivo:**  
Evitar cambios de schema que afecten Metabase y deployments; simplificar integración.

**Alternativas consideradas:**
1. Crear esquema explícito `gold`.
2. Separar marts en esquema adicional.

**Motivo de descarte:**
- Cambios adicionales de configuración.
- Complejidad innecesaria para entorno actual.

**Riesgo:**  
Menor pureza semántica.

**Mitigación:**  
Documentar `public` como capa Gold en arquitectura.

---

## ADR-007 - Prefect local para Orquestación

**Decisión:** Utilizar Prefect (server local + worker).

**Motivo:**  
Observabilidad, retries automáticos, parametrización por fecha (`dt`).

**Alternativas consideradas:**
1. Scripts secuenciales sin orquestador.
2. Cron jobs.
3. Apache Airflow.

**Motivo de descarte:**
- Scripts sueltos no escalan ni permiten monitoreo.
- Cron no ofrece trazabilidad estructurada.
- Airflow es más pesado para entorno local.

**Tradeoffs:**  
Requiere mantener server/worker activos, a cambio de control y visibilidad.

---

## ADR-008 - Metabase como capa de Consumo

**Decisión:** Integrar Metabase conectado a `public.*`.

**Motivo:**  
Validar modelo dimensional y exponer KPIs.

**Alternativas consideradas:**
1. Exportar CSV.
2. Usar notebooks.
3. No incluir capa de visualización.

**Motivo de descarte:**
- CSV no refleja entorno real.
- Notebooks no representan consumo de negocio.
- Sin BI no se valida el modelo analítico.

**Tradeoffs:**  
Gestión de volúmenes y configuración adicional, a cambio de arquitectura completa end-to-end.
