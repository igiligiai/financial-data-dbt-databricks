with clock as (
  select
    convert_timezone(current_timezone(), 'UTC', current_timestamp()) as checked_at
),

local_clock as (
  select
    checked_at,
    convert_timezone('UTC', 'Europe/Berlin', checked_at) as local_checked_at
  from clock
),

check_window as (
  select
    checked_at,
    case
      when weekday(local_checked_at) = 0 then
        convert_timezone(
          'Europe/Berlin',
          'UTC',
          local_checked_at - interval 3 days
        )
      else checked_at - interval 24 hours
    end as window_start
  from local_clock
),

file_inventory as (
  select
    'hourly_prices' as source_name,
    convert_timezone(current_timezone(), 'UTC', modificationTime) as modified_at
  from read_files(
    '/Volumes/api_data/raw/yfinance/hourly_prices',
    format => 'binaryFile',
    schema => 'path string, modificationTime timestamp, length bigint, content binary',
    fileNamePattern => '*.json'
  )
  union all
  select
    'company_info' as source_name,
    convert_timezone(current_timezone(), 'UTC', modificationTime) as modified_at
  from read_files(
    '/Volumes/api_data/raw/yfinance/company_info',
    format => 'binaryFile',
    schema => 'path string, modificationTime timestamp, length bigint, content binary',
    fileNamePattern => '*.json'
  )
),

source_status as (
  select
    w.checked_at,
    w.window_start,
    max(case when f.source_name = 'hourly_prices' then f.modified_at end)
      as hourly_prices_latest_modified_at,
    max(case when f.source_name = 'company_info' then f.modified_at end)
      as company_info_latest_modified_at,
    count_if(
      f.source_name = 'hourly_prices'
      and f.modified_at between w.window_start and w.checked_at
    ) as hourly_prices_fresh_files,
    count_if(
      f.source_name = 'company_info'
      and f.modified_at between w.window_start and w.checked_at
    ) as company_info_fresh_files
  from check_window as w
  left join file_inventory as f on true
  group by w.checked_at, w.window_start
)

select
  checked_at,
  window_start,
  hourly_prices_latest_modified_at,
  company_info_latest_modified_at,
  hourly_prices_fresh_files,
  company_info_fresh_files,
  case
    when hourly_prices_fresh_files > 0 and company_info_fresh_files > 0 then 1
    else 0
  end as should_run_dbt
from source_status
