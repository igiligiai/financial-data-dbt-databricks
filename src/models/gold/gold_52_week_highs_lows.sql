{{
  config(
    materialized='view'
  )
}}

with latest_prices as (
  select
    symbol,
    max_by(current_price, extracted_at)                                     as current_price,
    max_by(fifty_two_week_high, extracted_at)                               as fifty_two_week_high,
    max_by(fifty_two_week_low, extracted_at)                                as fifty_two_week_low,
    max(extracted_at)                                                       as latest_extracted_at
  from {{ ref('silver_hourly_prices') }}
  where extracted_at >= date_trunc('day', current_date())
    and extracted_at <  date_trunc('day', current_date() + interval 1 day)
  group by symbol
),

classified as (
  select
    symbol,
    current_price,
    fifty_two_week_high,
    fifty_two_week_low,
    latest_extracted_at,
    round(
      (current_price - fifty_two_week_low)
      / nullif(fifty_two_week_high - fifty_two_week_low, 0) * 100, 2
    )                                                                       as range_position_pct,
    case
      when current_price >= fifty_two_week_high * 0.95 then 'NEAR_52W_HIGH'
      when current_price <= fifty_two_week_low  * 1.05 then 'NEAR_52W_LOW'
      else 'MID_RANGE'
    end                                                                     as proximity_signal
  from latest_prices
  where fifty_two_week_high > 0
    and fifty_two_week_low  > 0
    and fifty_two_week_high > fifty_two_week_low
)

select
  c.symbol,
  c.company_name,
  c.sector,
  cl.current_price,
  cl.fifty_two_week_high,
  cl.fifty_two_week_low,
  cl.range_position_pct,
  cl.proximity_signal,
  cast(cl.latest_extracted_at as date)                                      as extracted_at_date
from classified as cl
join {{ ref('silver_company_info') }} as c
  on cl.symbol = c.symbol
where cl.proximity_signal != 'MID_RANGE'
order by cl.range_position_pct desc
