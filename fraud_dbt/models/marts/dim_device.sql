{{ config(materialized='table') }}

with base as (
  select distinct lower(trim(device)) as device
  from {{ ref('stg_transactions') }}
)

select
  md5(device) as device_id,
  device
from base
