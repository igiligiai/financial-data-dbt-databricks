{{
  config(
    materialized='view'
  )
}}

with market as (
  select * from {{ ref('gold_market_summary') }}
  where sector is not null
)

select
  sector,
  count(*)                                                  as num_companies,
  sum(market_cap)                                           as total_market_cap,
  round(avg(current_price), 2)                              as avg_price,
  round(avg(trailing_pe), 2)                                as avg_trailing_pe,
  round(avg(forward_pe), 2)                                 as avg_forward_pe,
  round(avg(profit_margins), 4)                             as avg_profit_margin,
  round(avg(gross_margins), 4)                              as avg_gross_margin,
  round(avg(dividend_yield), 4)                             as avg_dividend_yield,
  sum(volume)                                               as total_volume,
  round(
    sum(current_price * volume) / nullif(sum(volume), 0), 2
  )                                                         as vwap
from market
group by sector
