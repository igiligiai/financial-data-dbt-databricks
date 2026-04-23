{{
  config(
    materialized='view'
  )
}}

with latest_prices as (
  select
    *,
    row_number() over (
      partition by symbol
      order by extracted_at desc
    ) as _recency
  from {{ ref('silver_hourly_prices') }}
),

latest_company as (
  select
    *,
    row_number() over (
      partition by symbol
      order by extracted_at desc
    ) as _recency
  from {{ ref('silver_company_info') }}
)

select
  p.symbol,
  c.company_name,
  c.sector,
  c.industry,
  c.country,

  -- Price snapshot
  p.current_price,
  p.day_high,
  p.day_low,
  p.previous_close,
  p.volume,
  p.market_cap,

  -- Moving averages
  p.fifty_day_average,
  p.two_hundred_day_average,

  -- 52-week range
  p.fifty_two_week_high,
  p.fifty_two_week_low,

  -- Valuation
  c.trailing_pe,
  c.forward_pe,
  c.peg_ratio,
  c.trailing_eps,
  c.forward_eps,
  c.book_value,

  -- Dividends
  c.dividend_rate,
  c.dividend_yield,

  -- Analyst
  c.recommendation_key,
  c.number_of_analyst_opinions,

  -- Margins
  c.profit_margins,
  c.gross_margins,
  c.operating_margins,

  -- Metadata
  p.extracted_at   as price_extracted_at,
  c.extracted_at   as company_extracted_at

from latest_prices p
left join latest_company c
  on p.symbol = c.symbol
  and c._recency = 1
where p._recency = 1
