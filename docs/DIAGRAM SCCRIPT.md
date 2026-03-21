# Pipeline Execution Diagram (Scripts por Etapa)

## Vista End-to-End

```
                                      ┌─────────────────────────────┐
                                      │        ORQUESTACIÓN         │
                                      │  prefect_flow.py            │
                                      │  run_daily.py               │
                                      └──────────────┬──────────────┘
                                                     │
                                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                           1️⃣ SIMULACIÓN / INGESTA                           │
└──────────────────────────────────────────────────────────────────────────────┘
        │
        ├── simulate_arrival.py
        ├── generate_events.py
        │
        ▼
data/landing/dt=YYYY-MM-DD/
        │
        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       2️⃣ INICIALIZACIÓN CONTROL                             │
└──────────────────────────────────────────────────────────────────────────────┘
        │
        ├── init_batch_control.py
        │
        ▼
raw.batch_control
        │
        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                  3️⃣ PROMOCIÓN LANDING → BRONZE                             │
└──────────────────────────────────────────────────────────────────────────────┘
        │
        ├── (lógica interna dentro de prefect_flow.py
        │     y/o process_*_medallion.py)
        │
        ▼
data/bronze/
        │
        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                   4️⃣ PROCESAMIENTO MEDALLION                                │
└──────────────────────────────────────────────────────────────────────────────┘
        │
        ├── process_creditcard_medallion.py
        ├── process_events_medallion.py
        │
        ▼
PostgreSQL (Schema raw)
        ├── raw.creditcard_batches
        ├── raw.transaction_events
        ├── raw.creditcard_batches_quarantine
        ├── raw.transaction_events_quarantine
        └── raw.batch_control (actualización de status y conteos)
        │
        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                   5️⃣ TRANSFORMACIÓN ANALÍTICA (dbt)                         │
└──────────────────────────────────────────────────────────────────────────────┘
        │
        ├── fraud_dbt/
        │     ├── models/staging/
        │     ├── models/marts/
        │
        ├── dbt build
        │
        ▼
PostgreSQL
        ├── stg.*
        ├── public.fact_transactions
        ├── public.dim_*
        ├── public.mart_fraud_kpi
        └── public.mart_fraud_by_channel
        │
        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        6️⃣ CONSUMO BI                                         │
└──────────────────────────────────────────────────────────────────────────────┘
        │
        ├── Metabase
        │
        ▼
Dashboards y KPIs
```

---

## Resumen por Etapa

| Etapa | Scripts / Componentes |
|-------|------------------------|
| Simulación | simulate_arrival.py, generate_events.py |
| Inicialización | init_batch_control.py |
| Promoción a Bronze | Lógica interna en prefect_flow.py |
| Procesamiento Raw | process_creditcard_medallion.py, process_events_medallion.py |
| Control Operativo | raw.batch_control |
| Transformación Analítica | fraud_dbt (dbt build) |
| Orquestación | prefect_flow.py, run_daily.py |
| Visualización | Metabase |

---

## Flujo Simplificado

```
simulate_arrival.py
        ↓
init_batch_control.py
        ↓
process_*_medallion.py
        ↓
PostgreSQL (raw)
        ↓
dbt build (fraud_dbt)
        ↓
public.fact + marts
        ↓
Metabase
```
