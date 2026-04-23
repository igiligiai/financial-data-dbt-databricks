{{
  config(
    materialized='view'
  )
}}

with market as (
  select * from {{ ref('gold_market_summary') }}
)

select
  symbol,
  company_name,
  sector,
  industry,
  current_price,
  previous_close,

  -- Daily change
  round(current_price - previous_close, 2)                                  as price_change,
  round(
    (current_price - previous_close) / nullif(previous_close, 0) * 100, 2
  )                                                                         as price_change_pct,

  -- Moving average signals
  fifty_day_average,
  two_hundred_day_average,
  round(current_price - fifty_day_average, 2)                               as vs_50d_avg,
  round(current_price - two_hundred_day_average, 2)                         as vs_200d_avg,
  case
    when current_price > fifty_day_average
      and fifty_day_average > two_hundred_day_average then 'BULLISH'
    when current_price < fifty_day_average
      and fifty_day_average < two_hundred_day_average then 'BEARISH'
    else 'NEUTRAL'
  end                                                                       as trend_signal,

  -- 52-week range positioning (0 = at low, 100 = at high)
  fifty_two_week_low,
  fifty_two_week_high,
  round(
    (current_price - fifty_two_week_low)
    / nullif(fifty_two_week_high - fifty_two_week_low, 0) * 100, 2
  )                                                                         as range_52w_pct,

  -- Volume & market cap
  volume,
  market_cap,

  price_extracted_at

from market
