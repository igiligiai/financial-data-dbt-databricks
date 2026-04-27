{{
  config(
    materialized='view'
  )
}}

select
  symbol :: STRING                    as symbol,
  extracted_at :: STRING              as extracted_at,
  current_price :: STRING             as current_price,
  day_high :: STRING                  as day_high,
  day_low :: STRING                   as day_low,
  previous_close :: STRING            as previous_close,
  volume :: STRING                    as volume,
  market_cap :: STRING                as market_cap,
  fifty_day_average :: STRING         as fifty_day_average,
  two_hundred_day_average :: STRING   as two_hundred_day_average,
  fifty_two_week_high :: STRING       as fifty_two_week_high,
  fifty_two_week_low :: STRING        as fifty_two_week_low,
  _metadata.file_path                 as _source_file,
  _metadata.file_modification_time    as _file_modified_at
from read_files(
  '/Volumes/api_data/raw/yfinance/hourly_prices',
  format => 'json',
  multiLine => true
)
