{{
  config(
    materialized='view'
  )
}}

with daily_volume as (
  select
    symbol,
    cast(extracted_at as date)                                              as price_date,
    max_by(volume, extracted_at)                                            as daily_volume
  from {{ ref('silver_hourly_prices') }}
  where extracted_at >= date_trunc('day', current_date() - interval 21 day)
    and volume > 0
  group by symbol, cast(extracted_at as date)
),

volume_stats as (
  select
    symbol,
    count(distinct price_date)                                              as trading_days,
    avg(daily_volume)                                                       as avg_volume,
    stddev(daily_volume)                                                    as volume_stddev
  from daily_volume
  where price_date < date_trunc('day', current_date())
  group by symbol
  having count(distinct price_date) >= 5
),

today_volume as (
  select
    symbol,
    max_by(volume, extracted_at)                                            as today_volume
  from {{ ref('silver_hourly_prices') }}
  where extracted_at >= date_trunc('day', current_date())
    and extracted_at <  date_trunc('day', current_date() + interval 1 day)
    and volume > 0
  group by symbol
),

anomalies as (
  select
    t.symbol,
    t.today_volume,
    round(s.avg_volume)                                                     as avg_volume_21d,
    round(
      t.today_volume / nullif(s.avg_volume, 0), 2
    )                                                                       as volume_ratio,
    round(
      (t.today_volume - s.avg_volume) / nullif(s.volume_stddev, 0), 2
    )                                                                       as volume_z_score,
    s.trading_days
  from today_volume as t
  join volume_stats as s
    on t.symbol = s.symbol
  where t.today_volume > s.avg_volume * 1.5
)

select
  c.symbol,
  c.company_name,
  c.sector,
  a.today_volume,
  a.avg_volume_21d,
  a.volume_ratio,
  a.volume_z_score,
  a.trading_days
from anomalies as a
join {{ ref('silver_company_info') }} as c
  on a.symbol = c.symbol
order by a.volume_ratio desc
