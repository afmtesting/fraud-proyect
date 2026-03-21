# Architecture

## 1. Objetivo Arquitectónico

Implementar un pipeline end-to-end (ingesta → transformación → modelo dimensional → BI) aplicando prácticas reales de ingeniería de datos:

- Separación física por capas
- Idempotencia y control de batches
- Manejo de cuarentena
- Modelo dimensional para consumo analítico
- Orquestación y observabilidad

El diseño prioriza trazabilidad, reproducibilidad y orientación a producción.

---

## 2. Arquitectura Lógica

Patrón Medallion implementado en PostgreSQL mediante separación por esquemas:

```
raw  →  stg  →  public
(Bronze) (Silver) (Gold)
```

### raw (Bronze)
- Datos crudos
- Registro de batches
- Tablas de cuarentena
- Control operativo

### stg (Silver)
- Limpieza y normalización
- Estandarización de estructura
- Vistas de preparación

### public (Gold)
- Modelo dimensional (Star Schema)
- Tabla de hechos
- Dimensiones
- Marts analíticos listos para BI

---

## 3. Componentes y Responsabilidades

### Docker Compose
- Provisiona PostgreSQL (almacén analítico) y Metabase (BI).
- Asegura persistencia mediante volúmenes.
- Permite entorno reproducible y portable.

---

### Python
Responsable de la capa operativa.

- Generación de datos sintéticos (opcional).
- Manejo de filesystem: landing → bronze → quarantine.
- Carga controlada a `raw.*`.
- Actualización de `raw.batch_control`.
- Validaciones mínimas de integridad.

---

### dbt
Responsable del modelado analítico.

- Transformación de `raw.*` hacia `stg` y `public`.
- Construcción de modelo dimensional (fact + dims).
- Generación de marts agregados.
- Tests y reproducibilidad declarativa.

---

### Prefect (local)
Responsable de la orquestación.

- Coordinación de tasks.
- Control de dependencias.
- Retries automáticos.
- Logging centralizado.
- Scheduling opcional.

---

### Metabase
Responsable de la capa de consumo.

- Conexión directa al esquema `public`.
- Dashboards ejecutivos.
- Visualización de KPIs y análisis exploratorio.

---

## 4. Flujo End-to-End (Runtime)

1) **init_batch_control**
   - Garantiza existencia de `raw.batch_control`.
   - Operación idempotente.

2) **simulate_arrival (opcional)**
   - Genera archivos en `data/landing/dt=YYYY-MM-DD/`.

3) **promote_to_bronze**
   - Movimiento landing → bronze.
   - Evita procesamiento de archivos parciales.

4) **process_*_medallion**
   - Lectura de archivos bronze.
   - Validaciones básicas.
   - Inserción de registros válidos en `raw.*`.
   - Inserción de inválidos en `_quarantine`.
   - Actualización de `raw.batch_control`.

5) **dbt build**
   - Construcción de staging lógico.
   - Generación de modelo dimensional en `public`.
   - Creación de marts analíticos.

6) **Metabase**
   - Consulta sobre `public.fact_transactions` y `public.mart_*`.

---

## 5. Modelo Dimensional (public)

Implementación de Star Schema.

### fact_transactions
Grano definido por el modelo (fecha + dimensiones asociadas).

Contiene:
- amount_total
- tx_count
- fraud_tx_count
- fraud_rate

Claves foráneas:
- date_id
- time_id
- channel_id
- device_id
- merchant_id

---

### Dimensiones

- dim_date
- dim_time
- dim_channel
- dim_device
- dim_merchant

Separan atributos descriptivos de métricas, reduciendo redundancia y optimizando consultas analíticas.

---

### Marts

- mart_fraud_kpi
- mart_fraud_by_channel

Diseñados para consumo directo en dashboards.

---

## 6. Idempotencia y Control Operativo

`raw.batch_control` actúa como control plane del pipeline.

Contiene:

- dt
- batch_id
- source_file
- rows_valid
- rows_quarantined
- status
- error_message
- timestamps

Cada ejecución:
- Registra estado
- Permite re-procesamiento
- Evita duplicidad
- Garantiza trazabilidad

---

## 7. Gobernanza y Trazabilidad

- Separación física por esquemas.
- Tablas `_quarantine` conservan datos inválidos.
- Evidencia adicional en `data/quarantine/`.
- Control centralizado mediante `batch_control`.

Este diseño permite auditoría completa del proceso.

---

## 8. Observabilidad

- Prefect UI: monitoreo de runs y retries.
- Logs locales en carpeta `logs/`.
- Consultas SQL sobre `batch_control` y métricas.
- Validación analítica mediante Metabase.

---

## 9. Decisiones de Diseño

- Separación por esquemas en lugar de una sola capa.
- Control explícito de batches en vez de cargas directas.
- Modelo dimensional en lugar de tablas planas.
- Orquestación explícita en vez de scripts sueltos.
- Capa BI desacoplada del proceso ETL.

Estas decisiones buscan acercar el proyecto a un entorno real de producción.
