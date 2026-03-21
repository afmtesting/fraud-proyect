with base as (
  select
    dt::date as dt,
    time::bigint as time_sec,
    amount::numeric as amount,
    class::int as is_fraud
  from {{ source('raw', 'creditcard_batches') }}
)

select * from base
