{{ config(materialized='table') }}

with dates as (
  select distinct dt::date as dt
  from {{ ref('stg_transactions') }}
)

select
  to_char(dt, 'YYYYMMDD')::int as date_id,
  dt,
  extract(year from dt)::int as year,
  extract(month from dt)::int as month,
  extract(day from dt)::int as day
from dates
