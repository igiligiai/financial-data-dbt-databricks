-- Analysis: sector valuation comparison
-- Compiled but never materialized — use for ad-hoc exploration via dbt compile.
-- Ranks sectors by average forward P/E and compares against market-wide median.

with sector_stats as (
  select
    sector,
    count(*)                         as num_companies,
    round(avg(forward_pe), 2)        as avg_forward_pe,
    round(avg(trailing_pe), 2)       as avg_trailing_pe,
    round(avg(profit_margins), 4)    as avg_profit_margin,
    sum(market_cap)                  as total_market_cap
  from {{ ref('gold_market_summary') }}
  where sector is not null
    and forward_pe is not null
  group by sector
),

market_median as (
  select
    percentile_approx(forward_pe, 0.5) as median_forward_pe
  from {{ ref('gold_market_summary') }}
  where forward_pe is not null
)

select
  s.sector,
  s.num_companies,
  s.avg_forward_pe,
  s.avg_trailing_pe,
  s.avg_profit_margin,
  s.total_market_cap,
  m.median_forward_pe                                    as market_median_pe,
  round(s.avg_forward_pe - m.median_forward_pe, 2)       as pe_vs_market,
  case
    when s.avg_forward_pe < m.median_forward_pe then 'UNDERVALUED'
    when s.avg_forward_pe > m.median_forward_pe * 1.25 then 'PREMIUM'
    else 'FAIR'
  end                                                     as valuation_label
from sector_stats s
cross join market_median m
order by s.avg_forward_pe
