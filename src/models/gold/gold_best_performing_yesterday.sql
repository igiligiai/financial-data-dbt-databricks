{{
  config(
    materialized='view'
  )
}}

with best as (
  select
    symbol,
    max(extracted_at)                   as extracted_at,
    max_by(day_high, extracted_at)      as day_high,
    any_value(previous_close)           as previous_close
  from {{ ref('silver_hourly_prices') }}
  where cast(extracted_at as date) = (
    -- last completed trading day (e.g. Friday when run on Monday)
    select max(cast(extracted_at as date))
    from {{ ref('silver_hourly_prices') }}
    where cast(extracted_at as date) < current_date()
  )
  group by symbol
)

select
  c.symbol,
  c.company_name,
  c.sector,
  cast(b.extracted_at as date)                                              as extracted_at_date,
  date_format(b.extracted_at, 'HH:mm:ss')                                  as extracted_at_time,
  round(b.day_high - b.previous_close, 2)                                  as daily_gain_price,
  round(
    (b.day_high - b.previous_close) / nullif(b.previous_close, 0) * 100, 2
  )                                                                         as daily_gain_percentage
from best as b
join {{ ref('silver_company_info') }} as c
  on b.symbol = c.symbol
order by daily_gain_percentage desc
