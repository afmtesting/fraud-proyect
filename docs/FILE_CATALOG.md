# File Catalog (Python)

Este catálogo describe **para qué sirve cada script** y sus entradas/salidas.

---

## Entry points

### run_daily.py
**Rol:** Ejecutar el pipeline end-to-end manualmente.
- Orquesta: ingesta -> transform -> marts (dependiendo de tu implementación).
- Usa `DT` y `SIMULATE` como variables de entorno.

**Inputs:** env: `DT`, `SIMULATE`  
**Outputs:** inserciones en DB + modelos dbt en `public`.

---

### prefect_flow.py
**Rol:** Flow Prefect principal.
- Levanta docker (si aplica).
- Ejecuta `init_batch_control.py`, `run_daily.py`.
- Ejecuta dbt (`dbt deps` / `dbt build`).

**Inputs:** parámetros/vars Prefect + env `DT`, `SIMULATE`, `PROJECT_ROOT`  
**Outputs:** run observable en Prefect UI.

---

## Gobernanza

### init_batch_control.py
**Rol:** Bootstrap idempotente del control plane.
- Crea schema raw.
- Crea/asegura tabla `raw.batch_control` y columnas requeridas.

**Input:** env `PG_URL` (o default)  
**Output:** estructura en PostgreSQL

---

## Simulación y filesystem

### simulate_arrival.py
**Rol:** Generar datasets sintéticos en Landing.
- CSV creditcard (transacciones)
- JSONL events (telemetría/enrichment)
- Inyección opcional de registros inválidos para cuarentena (ver `docs/FAULT_INJECTION.md`).

**Inputs:** env `DT`, tamaños, `BAD_RATE_*` (opcional)  
**Outputs:** `data/landing/.../dt=.../`

---

### orchestration/promote_to_bronze.py
**Rol:** Promover Landing -> Bronze.
- Aísla la ingesta de archivos parciales.
- Estabiliza inputs para el procesamiento.

---

## Carga a RAW (DB)

### process_creditcard_medallion.py
**Rol:** Bronze -> RAW.
- Valida registros.
- Inserta válidos en `raw.creditcard_batches`.
- Inserta inválidos en `raw.creditcard_batches_quarantine`.
- Actualiza `raw.batch_control`.

---

### process_events_medallion.py
**Rol:** Bronze -> RAW.
- Parseo JSONL.
- Inserta válidos en `raw.transaction_events`.
- Inserta inválidos en `raw.transaction_events_quarantine`.
- Actualiza `raw.batch_control`.

---

## Legacy / opcional

### load_csv_to_pg.py
**Rol:** Loader directo CSV -> `stg.creditcard` (schema stg).
**Estado:** Legacy/auxiliar. No es parte del flujo principal si ya usas `raw.*` + dbt.

### generate_events.py
**Rol:** Generación alternativa de events, puede mezclar rutas DB/FS.
**Estado:** Legacy/experimental según implementación.
