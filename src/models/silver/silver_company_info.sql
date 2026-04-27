{{
  config(
    materialized='table'
  )
}}

with source as (
  select * from {{ ref('bronze_company_info') }}
  where error is null
),

typed as (
  select
    symbol,
    to_timestamp(extracted_at, 'yyyy-MM-dd HH:mm:ss')     as extracted_at,
    company_name,
    sector,
    industry,
    quote_type,
    long_business_summary,

    -- Location
    country,
    city,
    state,
    address,
    zip_code,
    phone,
    website,
    ir_website,

    -- Workforce
    cast(full_time_employees as int)                        as full_time_employees,

    -- Exchange
    exchange,
    full_exchange_name,
    currency,
    to_timestamp(regular_market_time)                       as regular_market_time,
    cast(regular_market_open as double)                     as regular_market_open,
    to_timestamp(first_trade_date)                          as first_trade_date,
    to_timestamp(last_split_date)                           as last_split_date,
    last_split_factor,

    -- Share structure
    cast(shares_outstanding as bigint)                      as shares_outstanding,
    cast(float_shares as bigint)                            as float_shares,
    cast(held_percent_insiders as double)                   as held_percent_insiders,
    cast(held_percent_institutions as double)               as held_percent_institutions,

    -- Risk
    cast(beta as double)                                    as beta,
    cast(audit_risk as int)                                 as audit_risk,
    cast(board_risk as int)                                 as board_risk,
    cast(compensation_risk as int)                          as compensation_risk,
    cast(share_holder_rights_risk as int)                   as share_holder_rights_risk,
    cast(overall_risk as int)                               as overall_risk,

    -- Dividends
    cast(dividend_rate as double)                            as dividend_rate,
    cast(dividend_yield as double)                           as dividend_yield,

    -- Analyst
    cast(number_of_analyst_opinions as int)                  as number_of_analyst_opinions,
    recommendation_key,

    -- Financials
    cast(total_revenue as bigint)                            as total_revenue,
    cast(total_debt as bigint)                               as total_debt,
    cast(total_cash as bigint)                               as total_cash,
    cast(free_cashflow as bigint)                            as free_cashflow,
    cast(book_value as double)                               as book_value,
    cast(revenue_growth as double)                           as revenue_growth,
    cast(earnings_growth as double)                          as earnings_growth,
    cast(profit_margins as double)                           as profit_margins,
    cast(gross_margins as double)                            as gross_margins,
    cast(operating_margins as double)                        as operating_margins,
    cast(return_on_assets as double)                         as return_on_assets,
    cast(return_on_equity as double)                         as return_on_equity,
    cast(trailing_pe as double)                              as trailing_pe,
    cast(forward_pe as double)                               as forward_pe,
    cast(peg_ratio as double)                                as peg_ratio,
    cast(trailing_eps as double)                             as trailing_eps,
    cast(forward_eps as double)                              as forward_eps,

    _source_file,
    _file_modified_at
  from source
  where symbol is not null
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

select * except(_row_num)
from deduplicated
where _row_num = 1
