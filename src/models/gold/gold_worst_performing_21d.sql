{{
  config(
    materialized='view'
  )
}}

with daily_prices as (
  select
    symbol,
    cast(extracted_at as date)                                              as price_date,
    max_by(current_price, extracted_at)                                     as closing_price,
    max_by(previous_close, extracted_at)                                    as previous_close
  from {{ ref('silver_hourly_prices') }}
  where extracted_at >= date_trunc('day', current_date() - interval 21 day)
  group by symbol, cast(extracted_at as date)
),

price_endpoints as (
  select
    symbol,
    max_by(closing_price, price_date)                                       as latest_price,
    min_by(closing_price, price_date)                                       as earliest_price,
    min(price_date)                                                         as period_start,
    max(price_date)                                                         as period_end,
    count(distinct price_date)                                              as trading_days
  from daily_prices
  group by symbol
  having count(distinct price_date) >= 2
)

select
  c.symbol,
  c.company_name,
  c.sector,
  p.period_start,
  p.period_end,
  p.trading_days,
  p.earliest_price,
  p.latest_price,
  round(p.latest_price - p.earliest_price, 2)                              as price_change,
  round(
    (p.latest_price - p.earliest_price) / nullif(p.earliest_price, 0) * 100, 2
  )                                                                         as price_change_pct
from price_endpoints as p
join {{ ref('silver_company_info') }} as c
  on p.symbol = c.symbol
order by price_change_pct asc
