with cc_by_sec as (
  select
    dt::date as dt,
    time_sec::bigint as time_sec,
    sum(amount)::numeric as amount_total,
    count(*)::bigint as tx_count,
    sum(case when is_fraud = 1 then 1 else 0 end)::bigint as fraud_tx_count
  from {{ ref('stg_creditcard') }}
  group by 1,2
),

-- 1 fila por (dt,time_sec) para mapear canal/device/merchant/risk_score
ev_map as (
  select dt::date as dt, time_sec::bigint as time_sec, channel, device, merchant, risk_score
  from (
    select
      dt,
      time_sec,
      coalesce(channel, 'unknown') as channel,
      coalesce(device, 'unknown') as device,
      coalesce(merchant, 'unknown') as merchant,
      risk_score,
      row_number() over (
        partition by dt, time_sec
        order by risk_score desc nulls last, merchant, device
      ) as rn
    from {{ ref('stg_transaction_events') }}
  ) x
  where rn = 1
),

joined as (
  select
    c.dt,
    c.time_sec,
    coalesce(e.channel, 'unknown') as channel,
    coalesce(e.device, 'unknown') as device,
    coalesce(e.merchant, 'unknown') as merchant,
    e.risk_score::numeric as risk_score,

    c.amount_total::numeric as amount_total,
    c.tx_count::bigint as tx_count,
    c.fraud_tx_count::bigint as fraud_tx_count
  from cc_by_sec c
  left join ev_map e
    on e.dt = c.dt
   and e.time_sec = c.time_sec
)

select *
from joined
