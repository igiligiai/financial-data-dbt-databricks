-- Singular test: ensure no hourly price record has a current_price
-- that falls outside the stock's 52-week range (with a 5% tolerance
-- for intraday overshoots). Flags data quality issues in the silver layer.

select
  symbol,
  extracted_at,
  current_price,
  fifty_two_week_low,
  fifty_two_week_high
from {{ ref('silver_hourly_prices') }}
where current_price is not null
  and fifty_two_week_low is not null
  and fifty_two_week_high is not null
  and (
    current_price < fifty_two_week_low * 0.95
    or current_price > fifty_two_week_high * 1.05
  )
