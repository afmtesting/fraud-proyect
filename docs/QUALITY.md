# Data Quality & Validation

Este documento describe las reglas de validación, manejo de registros inválidos y criterios de éxito del pipeline.

El objetivo es garantizar:

- Integridad de datos
- Trazabilidad
- Reproducibilidad
- Control de calidad operativo

---

# 1. Principios de Calidad Aplicados

- No descartar silenciosamente registros inválidos.
- Separar datos válidos e inválidos.
- Registrar métricas de calidad por batch.
- Permitir auditoría posterior.
- Mantener idempotencia en cada ejecución.

---

# 2. Reglas de Validación – Dataset Creditcard

Durante el procesamiento (`process_creditcard_medallion.py`) se aplican validaciones mínimas.

Ejemplos de validación:

- Campos obligatorios no nulos.
- Tipo de dato correcto (numéricos válidos).
- Rangos esperados (si aplica).
- Formato consistente de fecha.

Registros que no cumplen:

- Se insertan en `raw.creditcard_batches_quarantine`.
- Se registran como `rows_quarantined` en `raw.batch_control`.
- Se conserva evidencia en `data/quarantine/`.

---

# 3. Reglas de Validación – Eventos JSON

Durante el procesamiento de eventos:

- Validación de JSON bien formado.
- Presencia de campos requeridos.
- Tipos de dato consistentes.
- Valores dentro de rango esperado.

Eventos inválidos:

- Se insertan en `raw.transaction_events_quarantine`.
- Se registran en batch_control.

---

# 4. Criterio de Éxito de Batch

Un batch se considera exitoso cuando:

- Se procesan archivos correctamente.
- Se insertan registros válidos en `raw.*`.
- El proceso no lanza excepción crítica.
- `status = SUCCESS` en `raw.batch_control`.

Un batch puede tener registros en cuarentena y aún considerarse exitoso.

---

# 5. Métricas de Calidad Registradas

En `raw.batch_control` se almacenan:

- rows_valid
- rows_quarantined
- status
- error_message
- timestamps

Esto permite monitorear:

- Porcentaje de registros inválidos
- Tendencias de calidad por fecha
- Fallos recurrentes

---

# 6. Controles de Consistencia

Checks recomendados post-ejecución:

```sql
-- Verificar batches recientes
select * from raw.batch_control order by updated_ts desc;

-- Validar que fact tenga datos
select count(*) from public.fact_transactions;

-- Validar cuarentena
select count(*) from raw.creditcard_batches_quarantine;
```

---

# 7. Idempotencia

El pipeline:

- Registra ejecuciones por fecha (`dt`)
- Permite reprocesar sin duplicar datos
- Actualiza estado del batch

Esto evita inconsistencias al re-ejecutar procesos históricos.

---

# 8. Riesgos Identificados

- Validaciones mínimas pueden no capturar reglas de negocio complejas.
- No existe aún monitoreo automático de drift.
- No se implementa alertamiento automático.

---

# 9. Extensiones Futuras

- Implementar tests adicionales en dbt.
- Agregar métricas de calidad agregadas en mart.
- Integrar alertas automáticas.
- Implementar reglas de negocio más estrictas.
- Monitoreo de data drift si se agrega modelo ML.

---

# 10. Conclusión

La calidad de datos no depende únicamente de transformaciones,
sino de controles explícitos en la ingesta, validación, trazabilidad y registro de ejecución.

El diseño actual prioriza:

- Transparencia
- Auditabilidad
- Robustez
- Preparación para producción
