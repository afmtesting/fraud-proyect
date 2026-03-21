{{ config(materialized='table') }}

with s as (
  select
    dt::date as dt,
    time_sec::bigint as time_sec,
    lower(trim(channel)) as channel,
    lower(trim(device)) as device,
    lower(trim(merchant)) as merchant,
    risk_score::numeric as risk_score,
    amount_total::numeric as amount_total,
    tx_count::bigint as tx_count,
    fraud_tx_count::bigint as fraud_tx_count
  from {{ ref('stg_transactions') }}
  where time_sec between 0 and 86399
)

select
  -- grain: (dt, time_sec)
  md5(concat_ws('|', to_char(dt,'YYYY-MM-DD'), time_sec::text)) as transaction_id,

  to_char(dt,'YYYYMMDD')::int as date_id,
  time_sec::int as time_id,

  md5(channel) as channel_id,
  md5(device) as device_id,
  md5(merchant) as merchant_id,

  dt,
  time_sec,
  -- timestamp construido: dt + time_sec segundos
  (dt::timestamp + (time_sec::int * interval '1 second')) as transaction_ts,

  channel,
  device,
  merchant,

  risk_score,
  amount_total,
  tx_count,
  fraud_tx_count,

  (fraud_tx_count::numeric / nullif(tx_count, 0)) as fraud_rate
from s
