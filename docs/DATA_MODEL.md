# Data Catalog

Este catálogo describe **tablas reales** observadas en la base `frauddb`, su propósito operativo y su origen.
Para definiciones formales de grano, relaciones y métricas del modelo analítico ver: `DATA_MODEL.md`.

---

## Schema `raw` (Ingesta auditada)

### raw.batch_control
**Propósito:** Gobernanza y trazabilidad del pipeline (control plane).

**Grano:** 1 fila por lote (batch) y partición lógica (dt), por fuente.

**Uso:** Auditoría operativa, idempotencia y reprocesos.

**Consulta típica:**
```sql
select source, dt, status, rows_valid, rows_quarantined, source_file, updated_ts
from raw.batch_control
order by updated_ts desc;
```

---

### raw.creditcard_batches
**Propósito:** Persistir transacciones válidas del dataset creditcard.

**Grano:** 1 fila por transacción (registro original del CSV).

**Origen:** `data/bronze/creditcard/dt=.../*.csv`.

**Uso downstream:** fuente principal para modelos dbt que derivan `public.fact_transactions` y marts.

---

### raw.creditcard_batches_quarantine
**Propósito:** Capturar registros inválidos del CSV.

**Grano:** 1 fila por registro inválido.

**Uso:** control de calidad y diagnóstico de reglas de validación.

---

### raw.transaction_events
**Propósito:** Persistir eventos semiestructurados (JSONL) para enriquecer analítica.

**Grano:** 1 fila por evento.

**Origen:** `data/bronze/events/dt=.../*.jsonl`.

**Uso downstream:** construir/enriquecer dimensiones (channel/device/merchant) y fact.

---

### raw.transaction_events_quarantine
**Propósito:** Capturar eventos inválidos (JSON malformado o campos fuera de rango).

**Uso:** auditoría y mejora de validaciones.

---

## Schema `stg` (Staging / Silver)

### stg.creditcard
**Propósito:** Datos estandarizados para consumo del modelo analítico.

**Uso downstream:** preparación para fact/dims en `public`.

> Nota: detalles de grano, métricas y relaciones están en `DATA_MODEL.md`.

---

## Schema `public` (Capa Analítica Final / Gold)

### public.fact_transactions
**Propósito:** tabla central de hechos para analítica.

**Uso:** fuente principal para BI y marts.

> Ver `DATA_MODEL.md` para grano, métricas y llaves.

---

### public.dim_date
**Propósito:** dimensión calendario.

### public.dim_time
**Propósito:** dimensión de tiempo intradía.

### public.dim_channel
**Propósito:** dimensión de canal (web, mobile, pos, etc.).

### public.dim_device
**Propósito:** dimensión de dispositivo.

### public.dim_merchant
**Propósito:** dimensión de comercio/merchant.

---

### public.mart_fraud_kpi
**Propósito:** KPI diarios (panel ejecutivo).

### public.mart_fraud_by_channel
**Propósito:** fraude por canal y fecha.

---

### public.my_first_dbt_model
**Propósito:** modelo inicial de prueba.

**Recomendación:** eliminarlo si ya no aporta a la narrativa del proyecto o moverlo a carpeta de ejemplos para evitar ruido.
