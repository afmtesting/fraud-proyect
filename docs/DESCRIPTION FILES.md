# Scripts del Pipeline – Descripción Rápida

Este documento resume la función de cada script dentro del pipeline de fraude.

---

## simulate_arrival.py

**Propósito:**  
Simular la llegada diaria de archivos al directorio `data/landing/`.

**Uso:**  
Permite probar el pipeline como si recibiera datos reales por fecha (`dt`).

---

## generate_events.py

**Propósito:**  
Generar eventos sintéticos en formato JSONL.

**Uso:**  
Enriquecer el modelo con atributos como channel, device y merchant.

---

## init_batch_control.py

**Propósito:**  
Crear (si no existe) la tabla `raw.batch_control`.

**Uso:**  
Inicializar el control operativo del pipeline.  
Es idempotente.

---

## prefect_flow.py

**Propósito:**  
Definir el flujo completo del pipeline utilizando Prefect.

**Responsabilidades:**
- Orquestar etapas
- Controlar dependencias
- Ejecutar tareas en orden correcto
- Gestionar retries

---

## run_daily.py

**Propósito:**  
Ejecutar el pipeline para una fecha específica (`DT`).

**Uso:**  
Entrada manual o programada del proceso diario.

---

## process_creditcard_medallion.py

**Propósito:**  
Procesar los archivos CSV del dataset creditcard.

**Responsabilidades:**
- Leer archivos en `data/bronze/`
- Validar registros
- Insertar válidos en `raw.creditcard_batches`
- Insertar inválidos en `raw.creditcard_batches_quarantine`
- Actualizar `raw.batch_control`

Representa la lógica principal de la capa Bronze → Raw.

---

## process_events_medallion.py

**Propósito:**  
Procesar archivos JSON de eventos.

**Responsabilidades:**
- Validar estructura JSON
- Insertar válidos en `raw.transaction_events`
- Enviar inválidos a cuarentena
- Actualizar métricas del batch

Enriquece el modelo analítico.

---

## fraud_dbt/ (dbt build)

**Propósito:**  
Contener los modelos SQL declarativos del pipeline.

**Responsabilidades:**
- Transformar `raw` → `stg`
- Construir modelo dimensional en `public`
- Crear marts agregados
- Ejecutar tests de calidad

Representa la capa Silver → Gold.

---

## Metabase

**Propósito:**  
Capa de consumo analítico.

**Responsabilidades:**
- Consultar `public.fact_transactions`
- Visualizar KPIs
- Exponer métricas en dashboards

---

# Resumen General

| Tipo | Función |
|------|----------|
| simulate | Genera datos de prueba |
| init | Inicializa control operativo |
| process | Limpia y carga datos en raw |
| dbt | Construye modelo analítico |
| prefect | Orquesta ejecución |
| metabase | Visualiza resultados |
