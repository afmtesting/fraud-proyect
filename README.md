# Fraud Detection Data Pipeline  
**Proyecto de Ingeniería de Datos Orientado a Producción**

---

## 1. Objetivo

Diseñar e implementar un pipeline de datos reproducible, orquestado y trazable , utilizando principios de arquitectura de datos y prácticas orientadas a producción.

Este proyecto demuestra:

- Diseño de arquitectura escalable  
- Separación clara de responsabilidades  
- Control de batches e idempotencia  
- Observabilidad y trazabilidad  
- Modelo dimensional listo para analítica  
- Capa de consumo mediante dashboard  

---

## 2. Arquitectura Implementada

La arquitectura sigue el patrón Medallion, implementado físicamente mediante separación por esquemas en PostgreSQL:

```
raw  →  stg  →  public
(Bronze) (Silver) (Gold)
```

### raw (Bronze)

Responsable de la ingesta y control.

- Datos crudos
- Registro de batches
- Tablas de cuarentena
- Trazabilidad completa

### stg (Silver)

Responsable de transformación y estandarización.

- Limpieza de datos
- Normalización
- Validaciones
- Vistas de staging

### public (Gold)

Capa analítica dimensional.

- Tablas de dimensiones
- Tabla de hechos
- Marts agregados
- KPIs 

Esta implementación garantiza aislamiento entre capas, reprocesamiento seguro y separación clara entre datos operativos y analíticos.

---

## 3. Stack Tecnológico Utilizado

### Lenguaje
- **Python 3.x**

Utilizado para:
- Ingesta de datos
- Simulación de eventos
- Procesamiento intermedio
- Integración con PostgreSQL
- Orquestación con Prefect

---

### Base de Datos
- **PostgreSQL**

Utilizado para:
- Implementación física de arquitectura Medallion
- Separación por esquemas (`raw`, `stg`, `public`)
- Modelo dimensional (star schema)
- Persistencia de métricas y marts analíticos

---

### Orquestación
- **Prefect**

Utilizado para:
- Definición de flujos
- Control de dependencias
- Manejo de reintentos
- Ejecuciones programadas

---

### Transformaciones Analíticas
- **dbt (Data Build Tool)**

Utilizado para:
- Modelado SQL versionado
- Construcción de dimensiones y fact table
- Creación de marts analíticos
- Testing y documentación de modelos

---

### Visualización y Analítica
- **Metabase**

Utilizado para:
- Conexión directa al esquema `public`
- Construcción de dashboards
- Visualización de KPIs 
- Análisis exploratorio sobre modelo dimensional

Justificación:
Metabase permite una capa ligera de BI conectada directamente al modelo dimensional, validando que la arquitectura esté correctamente diseñada para consumo analítico.

---

### Contenerización
- **Docker & Docker Compose**

Utilizado para:
- Entorno reproducible
- Levantamiento de servicios (PostgreSQL + Metabase)
- Ejecución consistente del pipeline

---

### Librerías Python Relevantes

- `pandas`
- `sqlalchemy`
- `psycopg2`
- `prefect`

---

## 4. Flujo del Pipeline

1. Simulación o recepción de eventos
2. Registro del batch en `raw.batch_control`
3. Ingesta en tablas raw
4. Validación y envío a cuarentena si aplica
5. Transformación hacia stg
6. Construcción de modelo dimensional en public
7. Generación de marts y KPIs
8. Consumo analítico mediante Metabase

Cada ejecución queda registrada y puede reprocesarse sin generar duplicados.

---

## 5. Esquema de Base de Datos

### raw

Tablas:
- batch_control
- creditcard_batches
- creditcard_batches_quarantine
- transaction_events
- transaction_events_quarantine

Responsabilidad:
- Ingesta
- Control
- Cuarentena
- Trazabilidad

---

### stg

Tablas:
- creditcard

Vistas:
- stg_creditcard
- stg_transactions
- stg_transaction_events

Responsabilidad:
- Limpieza
- Estandarización
- Preparación estructural

---

### public

Dimensiones:
- dim_channel
- dim_date
- dim_device
- dim_merchant
- dim_time

Tabla de hechos:
- fact_transactions

Marts:
- mart_fraud_by_channel
- mart_fraud_kpi

Responsabilidad:
- Modelo dimensional
- KPIs agregados
- Datos listos para dashboards

---

## 6. Estructura del Proyecto

```
├── data/                      # Archivos de entrada
├── docs/                      # Documentación técnica
├── flows/                     # Flujos Prefect
├── orchestration/             # Lógica de orquestación
├── fraud_dbt/                 # Modelos dbt
├── logs/                      # Logs de ejecución
├── docker-compose.yml         # Configuración de contenedores
├── prefect_flow.py            # Flow principal
├── load_csv_to_pg.py          # Ingesta a PostgreSQL
├── process_creditcard_medallion.py
├── process_events_medallion.py
├── init_batch_control.py
├── simulate_arrival.py
├── run_daily.py


