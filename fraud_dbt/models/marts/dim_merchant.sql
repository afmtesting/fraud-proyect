{{ config(materialized='table') }}

with base as (
  select distinct lower(trim(merchant)) as merchant
  from {{ ref('stg_transactions') }}
)

select
  md5(merchant) as merchant_id,
  merchant
from base
