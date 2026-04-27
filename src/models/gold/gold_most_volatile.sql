{{
  config(
    materialized='view'
  )
}}

with intraday_ranges as (
  select
    symbol,
    cast(extracted_at as date)                                              as price_date,
    max(day_high)                                                           as day_high,
    min(day_low)                                                            as day_low,
    max_by(current_price, extracted_at)                                     as closing_price
  from {{ ref('silver_hourly_prices') }}
  where extracted_at >= date_trunc('day', current_date() - interval 21 day)
    and day_high > 0
    and day_low > 0
  group by symbol, cast(extracted_at as date)
),

volatility as (
  select
    symbol,
    count(distinct price_date)                                              as trading_days,
    round(avg(day_high - day_low), 2)                                       as avg_intraday_range,
    round(
      avg((day_high - day_low) / nullif(day_low, 0) * 100), 2
    )                                                                       as avg_intraday_range_pct,
    round(max(day_high - day_low), 2)                                       as max_intraday_range,
    round(
      max((day_high - day_low) / nullif(day_low, 0) * 100), 2
    )                                                                       as max_intraday_range_pct,
    round(stddev(closing_price), 2)                                         as price_stddev,
    round(
      stddev(closing_price) / nullif(avg(closing_price), 0) * 100, 2
    )                                                                       as coefficient_of_variation
  from intraday_ranges
  group by symbol
  having count(distinct price_date) >= 2
)

select
  c.symbol,
  c.company_name,
  c.sector,
  v.trading_days,
  v.avg_intraday_range,
  v.avg_intraday_range_pct,
  v.max_intraday_range,
  v.max_intraday_range_pct,
  v.price_stddev,
  v.coefficient_of_variation
from volatility as v
join {{ ref('silver_company_info') }} as c
  on v.symbol = c.symbol
order by v.avg_intraday_range_pct desc
