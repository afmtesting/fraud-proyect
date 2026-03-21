with base as (
  select
    dt,
    transaction_time::bigint as time_sec,
    payload->>'channel' as channel,
    payload->>'device' as device,
    payload->>'merchant' as merchant,
    (payload->>'risk_score')::numeric as risk_score
  from {{ source('raw', 'transaction_events') }}
)

select *
from base
