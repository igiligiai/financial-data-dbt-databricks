{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol', 'extracted_at'],
    on_schema_change='append_new_columns'
  )
}}

with source as (
  select * from {{ ref('bronze_hourly_prices') }}
  {% if is_incremental() %}
  where to_timestamp(extracted_at, 'yyyy-MM-dd HH:mm:ss') >= (
    select coalesce(max(extracted_at), to_timestamp('1900-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')) - interval 2 days
    from {{ this }}
  )
  {% endif %}
),

typed as (
  select
    symbol,
    to_timestamp(extracted_at, 'yyyy-MM-dd HH:mm:ss')     as extracted_at,
    cast(current_price as double)                         as current_price,
    cast(day_high as double)                              as day_high,
    cast(day_low as double)                               as day_low,
    cast(previous_close as double)                        as previous_close,
    cast(volume as bigint)                                as volume,
    cast(market_cap as bigint)                            as market_cap,
    cast(fifty_day_average as double)                     as fifty_day_average,
    cast(two_hundred_day_average as double)               as two_hundred_day_average,
    cast(fifty_two_week_high as double)                   as fifty_two_week_high,
    cast(fifty_two_week_low as double)                    as fifty_two_week_low,
    _source_file,
    _file_modified_at
  from source
  where symbol is not null
    and current_price is not null
    and day_high is not null
    and day_low is not null
),

deduplicated as (
  select
    *,
    row_number() over (
      partition by symbol, extracted_at
      order by _file_modified_at desc
    ) as _row_num
  from typed
)

select
  symbol,
  extracted_at,
  current_price,
  day_high,
  day_low,
  previous_close,
  volume,
  market_cap,
  fifty_day_average,
  two_hundred_day_average,
  fifty_two_week_high,
  fifty_two_week_low,
  _source_file,
  _file_modified_at
from deduplicated
where _row_num = 1
