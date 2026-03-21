{{ config(materialized='table') }}

select
  dt,
  channel,
  sum(tx_count) as tx_count,
  sum(fraud_tx_count) as fraud_tx_count,
  (sum(fraud_tx_count)::numeric / nullif(sum(tx_count), 0)) as fraud_rate
from {{ ref('fact_transactions') }}
group by 1,2
