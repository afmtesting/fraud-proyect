{{ config(materialized='table') }}

-- Dimensión tiempo a nivel segundo (0..86399)
with sec as (
  select generate_series(0, 86399) as time_sec
)
select
  time_sec::int as time_id,
  make_time(
    (time_sec / 3600)::int,
    ((time_sec % 3600) / 60)::int,
    (time_sec % 60)::int
  ) as time_value,
  (time_sec / 3600)::int as hour,
  ((time_sec % 3600) / 60)::int as minute,
  (time_sec % 60)::int as second
from sec
