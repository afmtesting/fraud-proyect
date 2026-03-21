# Pipeline Runbook

Guía de operación, ejecución y troubleshooting del pipeline de fraude.

---

# 1. Requisitos

- Docker / Docker Compose
- Python 3.x
- Entorno virtual activado
- dbt instalado
- Prefect instalado

---

# 2. Levantar Infraestructura

```bash
docker compose up -d
```

Servicios:

- PostgreSQL
- Metabase

Verificar:

```bash
docker compose ps
```

---

# 3. Ejecución Manual (Baseline)

```powershell
venv\Scripts\activate
$env:DT="2026-02-15"
$env:SIMULATE="1"

python init_batch_control.py
python run_daily.py

cd fraud_dbt
dbt build
cd ..
```

Variables:

- `DT` → Fecha a procesar
- `SIMULATE` → 1 genera eventos sintéticos, 0 usa datos existentes

---

# 4. Operación con Prefect

## Levantar servidor

```bash
prefect server start
```

## Levantar worker

```bash
prefect worker start --pool fraud-pool
```

UI:

```
http://127.0.0.1:4200
```

---

# 5. Flujo Interno

1. Registro en `raw.batch_control`
2. Landing → Bronze
3. Procesamiento e inserción en `raw.*`
4. Registros inválidos → `_quarantine`
5. `dbt build`
6. Actualización de marts en `public`
7. Consumo vía Metabase

---

# 6. Checks Operativos

## 6.1 Estado de lotes

```sql
select source, dt, status, rows_valid, rows_quarantined, source_file, updated_ts
from raw.batch_control
order by updated_ts desc;
```

Validar:

- status = SUCCESS
- rows_valid > 0

---

## 6.2 Conteos raw vs fact (sanity check)

```sql
select count(*) as raw_creditcard from raw.creditcard_batches;
select count(*) as fact_rows from public.fact_transactions;
```

---

## 6.3 Cuarentena

```sql
select dt, count(*) from raw.creditcard_batches_quarantine group by 1 order by 1 desc;
select dt, count(*) from raw.transaction_events_quarantine group by 1 order by 1 desc;
```

Filesystem:

```
data/quarantine/
```

---

# 7. Re-proceso Histórico

## Manual

```powershell
$env:DT="2026-02-10"
$env:SIMULATE="0"

python run_daily.py

cd fraud_dbt
dbt build
```

---

## Prefect (con parámetro dt)

```powershell
prefect deployment run "fraud-daily-pipeline/fraud-daily" --params '{"dt":"2026-02-10"}'
```

---

# 8. Troubleshooting Rápido

## A) No crea carpetas landing/bronze

- Verificar `DT`
- Verificar `SIMULATE`
- Revisar logs en Prefect UI
- Confirmar `working_dir` del deployment

---

## B) Falla conexión PostgreSQL

```bash
docker compose ps
```

Validar:

- Puerto expuesto
- Variable `PG_URL`
- Conectividad con `psql`

Reiniciar:

```bash
docker compose restart
```

---

## C) dbt falla

```bash
cd fraud_dbt
dbt debug
dbt build
```

Revisar:

- `profiles.yml`
- target schema
- errores de modelo

---

# 9. Persistencia Metabase

⚠ Importante:

No ejecutar:

```bash
docker compose down -v
```

Eso elimina el volumen y borra dashboards.

Usar únicamente:

```bash
docker compose down
```

---

# 10. Idempotencia y Seguridad Operativa

- `raw.batch_control` es el control plane.
- No insertar manualmente en `public.*`.
- No modificar tablas de producción manualmente.
- Reprocesar siempre mediante el pipeline.

---

# 11. Recuperación Completa

Si la base queda inconsistente:

```bash
docker compose down -v
docker compose up -d
python init_batch_control.py
```

Luego reprocesar fecha deseada.

---

# 12. Extensión a Producción

Migración natural hacia:

- PostgreSQL → RDS
- Prefect local → Prefect Cloud
- dbt local → dbt Cloud
- Logs → sistema centralizado
- Monitoreo → alertas automáticas

---

# 13. Principio Operativo

El pipeline está diseñado para:

- Ser reproducible
- Ser auditable
- Soportar reprocesamiento
- Separar operación y analítica
- Facilitar consumo mediante BI

No es un conjunto de scripts, sino un sistema controlado.
