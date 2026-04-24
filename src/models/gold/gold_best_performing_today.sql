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
  where extracted_at >= date_trunc('day', current_date())
    and extracted_at <  date_trunc('day', current_date() + interval 1 day)
  group by symbol
),

ranked as (
  select
    symbol,
    extracted_at,
    day_high,
    previous_close,
    round(day_high - previous_close, 2)                           as daily_gain_price,
    round(
      (day_high - previous_close) / nullif(previous_close, 0) * 100, 2
    )                                                             as daily_gain_percentage
  from best
  order by daily_gain_percentage desc
)

select
  c.symbol,
  c.company_name,
  c.sector,
  cast(r.extracted_at as date)                                              as extracted_at_date,
  date_format(r.extracted_at, 'HH:mm:ss')                                  as extracted_at_time,
  r.daily_gain_price,
  r.daily_gain_percentage
from ranked as r
join {{ ref('silver_company_info') }} as c
  on r.symbol = c.symbol
order by r.daily_gain_percentage desc
