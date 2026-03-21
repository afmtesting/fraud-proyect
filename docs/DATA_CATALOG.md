# Data Catalog

Este catálogo documenta las **tablas físicas reales** en la base `frauddb`, su propósito operativo y su origen.

Para definiciones formales de:
- Grano del modelo
- Métricas
- Relaciones entre tablas
- Justificación del Star Schema

Ver: `DATA_MODEL.md`.

---

# Schema `raw` (Ingesta Auditada / Bronze)

Capa responsable de persistir datos crudos y controlar ejecuciones.

---

## raw.batch_control

**Propósito:** Control operativo del pipeline (control plane).

**Grano:** 1 fila por lote (batch) y partición lógica (`dt`), por fuente.

**Responsabilidades:**
- Idempotencia
- Registro de estado (SUCCESS / FAILED)
- Conteos válidos vs cuarentena
- Auditoría de ejecuciones

**Consulta típica:**
```sql
select source, dt, status, rows_valid, rows_quarantined, source_file, updated_ts
from raw.batch_control
order by updated_ts desc;
```

---

## raw.creditcard_batches

**Propósito:** Persistir transacciones válidas del dataset creditcard.

**Grano:** 1 fila por transacción original del CSV.

**Origen:**
```
data/bronze/creditcard/dt=YYYY-MM-DD/*.csv
```

**Uso downstream:**
Fuente principal para transformaciones dbt que derivan:
- `public.fact_transactions`
- marts analíticos

---

## raw.creditcard_batches_quarantine

**Propósito:** Capturar registros inválidos del CSV.

**Grano:** 1 fila por registro inválido.

**Uso:**
- Diagnóstico de reglas de validación
- Auditoría de calidad de datos
- Revisión manual si aplica

---

## raw.transaction_events

**Propósito:** Persistir eventos semiestructurados (JSONL) para enriquecer el modelo.

**Grano:** 1 fila por evento.

**Origen:**
```
data/bronze/events/dt=YYYY-MM-DD/*.jsonl
```

**Uso downstream:**
- Construcción y enriquecimiento de dimensiones
- Atributos adicionales para fact

---

## raw.transaction_events_quarantine

**Propósito:** Capturar eventos inválidos (JSON malformado o campos fuera de rango).

**Uso:**
- Auditoría
- Mejora continua de validaciones

---

# Schema `stg` (Staging / Silver)

Capa intermedia de estandarización y limpieza.

---

## stg.creditcard

**Propósito:** Datos normalizados listos para modelado analítico.

**Responsabilidad:**
- Limpieza
- Estandarización
- Preparación para fact/dims

> Detalles de grano y relaciones están documentados en `DATA_MODEL.md`.

---

# Schema `public` (Capa Analítica / Gold)

Capa final orientada a consumo BI.

---

## public.fact_transactions

**Propósito:** Tabla central de hechos para analítica.

**Uso:**
- Fuente principal para dashboards
- Base para marts agregados

> Ver `DATA_MODEL.md` para métricas, llaves y grano.

---

## Dimensiones

- public.dim_date  
- public.dim_time  
- public.dim_channel  
- public.dim_device  
- public.dim_merchant  

**Propósito:** Atributos descriptivos que enriquecen la fact.

---

## Marts Analíticos

### public.mart_fraud_kpi
KPI diario para panel ejecutivo.

### public.mart_fraud_by_channel
Fraud rate por canal y fecha.

---

# Nota sobre Modelos
