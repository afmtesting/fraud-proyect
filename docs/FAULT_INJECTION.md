# Fault Injection (simulación de registros inválidos -> cuarentena)

## Objetivo
Inyectar una pequeña proporción de registros malformados en los datasets generados (CSV y JSONL) para:
- Validar la lógica de cuarentena
- Demostrar gobernanza sin “romper” el pipeline
- Mantener el flujo end-to-end (no debe detenerse)

## Diseño
La simulación debe crear registros inválidos **pero controlados**:
- CSV creditcard:
  - `amount` negativo o null
  - `time` fuera de rango
  - `class` fuera de {0,1}
- JSONL events:
  - JSON malformado (línea no-JSON)
  - `risk_score` fuera de [0,1]
  - falta `transaction_time`

La carga a RAW debe:
- Enviar registros inválidos a tablas `_quarantine`
- Registrar conteos en `raw.batch_control`
- Continuar con los registros válidos

## Implementación recomendada (rápida, no disruptiva)

### 1) En `simulate_arrival.py` agregar tasas de error
Agregar env vars:
- `BAD_RATE_CSV` (default 0.01)
- `BAD_RATE_EVENTS` (default 0.01)

Y durante la generación:
- Cada N filas, inyectar 1 fila inválida.

### 2) En loaders (`process_*_medallion.py`)
Asegurar:
- Try/except por registro (no por archivo completo)
- Acumular inválidos en memoria o escribir a quarantine incrementalmente
- Actualizar `rows_valid` y `rows_quarantined` al final
- Status `LOADED` aunque haya cuarentena (si hay válidos)

## Patch sugerido (extracto orientativo)

### simulate_arrival.py (pseudo-código)
- Para CSV:
  - Si `random() < BAD_RATE_CSV`: set amount=-1 o class=3
- Para JSONL:
  - Si `random() < BAD_RATE_EVENTS`: escribir una línea "NOT_JSON"
  - o `risk_score=1.7`

> Nota: mantener `BAD_RATE_*` bajo (0.5% - 2%) para que el pipeline se vea estable.
