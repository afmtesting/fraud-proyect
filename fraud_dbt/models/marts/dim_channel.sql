{{ config(materialized='table') }}

with base as (
  select distinct lower(trim(channel)) as channel
  from {{ ref('stg_transactions') }}
)

select
  md5(channel) as channel_id,
  channel
from base
