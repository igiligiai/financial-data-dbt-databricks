# instructions.md — Project Guide

## Project Overview

**financial_data_dbt_databricks** is a dbt project running on Databricks that ingests hourly stock market data from the Yahoo Finance API, transforms it through a medallion architecture (bronze → silver → gold), and produces analytical views for financial reporting.

### Tech Stack

- **dbt-databricks** — transformation framework
- **Databricks Unity Catalog** — storage and governance
- **Delta Lake** — table format with ZORDER optimisation
- **yfinance** — data source (Python ingestion)
- **Databricks Asset Bundles** — deployment and job orchestration
- **uv** — Python package manager (run dbt via `uv run dbt ...`)

### Data Flow

```
Yahoo Finance API
  → /Volumes/api_data/raw/yfinance/  (JSON files on Unity Catalog Volumes)
    → bronze (views — raw reads with :: STRING casting)
      → silver (incremental tables — typed, deduplicated, ZORDER optimised)
        → gold (views — analytical models joined with company info)
```

### Directory Structure

```
src/
  models/
    sources/        _sources.yml
    bronze/         bronze_*.sql, _bronze__schema.yml
    silver/         silver_*.sql, _silver__schema.yml
    gold/           gold_*.sql,   _gold__schema.yml
  tests/            Singular SQL tests
  macros/           Custom Jinja macros
  analyses/         Ad-hoc analytical queries
  seeds/            CSV seed data
  snapshots/        SCD snapshots
resources/          Databricks Asset Bundle job definitions
dbt_profiles/       Production profile overrides
```

---

## Medallion Architecture Rules

### Bronze

- **Materialised as**: `view`
- **Purpose**: Raw reads from Unity Catalog Volumes via `read_files()`
- **Rules**:
  - Cast every column to `:: STRING` — no type coercion here
  - Expose `_metadata.file_path` as `_source_file` and `_metadata.file_modification_time` as `_file_modified_at`
  - No filtering, no deduplication, no business logic

### Silver

- **Materialised as**: `incremental` (merge strategy) or `table`
- **Purpose**: Typed, validated, deduplicated records
- **Rules**:
  - Always cast columns to their proper types (`double`, `bigint`, `int`, `timestamp`)
  - Always deduplicate using `row_number() over (partition by ... order by _file_modified_at desc)`
  - Filter out null keys and required fields in the `typed` CTE
  - Use the standard CTE pipeline: `source` → `typed` → `deduplicated` → final select
  - For incremental models, filter source with `{% if is_incremental() %}` on `extracted_at`
  - Apply `OPTIMIZE ... ZORDER BY` via `post_hook` — do **not** use `partition_by`

### Gold

- **Materialised as**: `view`
- **Purpose**: Business-facing analytics and aggregations
- **Rules**:
  - Reference silver models via `{{ ref('silver_*') }}`
  - Keep heavy filtering/aggregation in CTEs **before** joining dimension tables
  - Use `nullif(denominator, 0)` for all division to prevent divide-by-zero errors
  - Apply `round()` to all calculated numeric outputs

---

## SQL Coding Conventions

### Keywords & Casing

```sql
-- ✅ Correct: lowercase SQL keywords
select
  symbol,
  cast(price as double) as price
from {{ ref('silver_hourly_prices') }}
where symbol is not null
group by symbol
order by symbol

-- ❌ Wrong: uppercase SQL keywords
SELECT
  symbol,
  CAST(price AS DOUBLE) AS price
FROM {{ ref('silver_hourly_prices') }}
WHERE symbol IS NOT NULL
```

**Exception**: Data types with `::` casting in bronze are UPPERCASE — `:: STRING`, `:: DOUBLE`, `:: BIGINT`.

### Indentation

- **2 spaces** everywhere — SQL, YAML, Jinja
- Never use tabs

### Column Alignment

Align `as` aliases to a consistent column using space padding for readability:

```sql
-- ✅ Correct: aligned aliases
select
  symbol,
  max(extracted_at)                   as extracted_at,
  max_by(day_high, extracted_at)      as day_high,
  any_value(previous_close)           as previous_close

-- ❌ Wrong: no alignment
select
  symbol,
  max(extracted_at) as extracted_at,
  max_by(day_high, extracted_at) as day_high,
  any_value(previous_close) as previous_close
```

### CTEs

- Use descriptive lowercase `snake_case` names
- Standard pipeline names: `source`, `typed`, `deduplicated`
- Domain-specific names: `daily_prices`, `volume_stats`, `latest_company`, `anomalies`
- No trailing comma after the last CTE before `select`

```sql
-- ✅ Correct
with source as (
  select * from {{ ref('bronze_hourly_prices') }}
),

typed as (
  ...
)

select ...

-- ❌ Wrong: trailing comma on last CTE
with source as (
  ...
),

typed as (
  ...
),  -- ← This causes PARSE_SYNTAX_ERROR

select ...
```

### JOINs

```sql
-- ✅ Correct: 2-space indent on ON clause, table aliases
from prices as p
join {{ ref('silver_company_info') }} as c
  on p.symbol = c.symbol

-- ❌ Wrong: ON on same line, no aliases
from prices JOIN silver_company_info ON prices.symbol = silver_company_info.symbol
```

### WHERE Clauses

```sql
-- ✅ Correct: conditions on separate lines with 2-space continuation
where symbol is not null
  and current_price is not null
  and day_high is not null

-- ❌ Wrong: everything on one line
where symbol is not null and current_price is not null and day_high is not null
```

### CASE Statements

```sql
-- ✅ Correct: lowercase keywords, 2-space indent for conditions
case
  when current_price >= fifty_two_week_high * 0.95 then 'NEAR_52W_HIGH'
  when current_price <= fifty_two_week_low  * 1.05 then 'NEAR_52W_LOW'
  else 'MID_RANGE'
end as proximity_signal
```

### Window Functions

```sql
-- ✅ Correct: multi-line with indentation
row_number() over (
  partition by symbol, extracted_at
  order by _file_modified_at desc
) as _row_num
```

### Calculations

- Always wrap division with `nullif(denominator, 0)`
- Always `round()` computed outputs
- Break complex expressions across lines

```sql
-- ✅ Correct
round(
  (day_high - previous_close) / nullif(previous_close, 0) * 100, 2
)                                                         as daily_gain_pct

-- ❌ Wrong: no nullif guard, no rounding
(day_high - previous_close) / previous_close * 100 as daily_gain_pct
```

---

## Jinja Conventions

### Config Blocks

Always at the very top of the file, before any SQL:

```jinja2
{{
  config(
    materialized='view'
  )
}}
```

- Outer braces `{{` and `}}` on their own lines
- 2-space indent for `config(`, 4-space indent for parameters

### Model References

```jinja2
-- ✅ Correct
from {{ ref('silver_hourly_prices') }}

-- ❌ Wrong: hardcoded table names
from workspace.finance.silver_hourly_prices
```

### Incremental Logic

```jinja2
{% if is_incremental() %}
where to_timestamp(extracted_at, 'yyyy-MM-dd HH:mm:ss') > (
  select max(extracted_at) from {{ this }}
)
{% endif %}
```

---

## YAML Schema Conventions

### File Naming

- Schema files: `_{layer}__schema.yml` (note the double underscore)
- Sources file: `_sources.yml`

### Structure

```yaml
version: 2

models:
  - name: model_name
    description: "Concise description of the model"
    columns:
      - name: column_name
        description: "What this column represents"
        data_tests:
          - not_null
          - unique
```

### Rules

- Use `data_tests:` (not the deprecated `tests:`)
- Descriptions use **double quotes**
- Every model must have at least a `description`
- Key columns (symbol, sector, etc.) should have `not_null` tests
- Unique identifiers should have `unique` tests
- Enum-like columns should have `accepted_values` tests:

```yaml
data_tests:
  - accepted_values:
      arguments:
        values: ['BULLISH', 'BEARISH', 'NEUTRAL']
```

---

## File Naming Conventions

| Layer | SQL File | Schema File |
|-------|----------|-------------|
| Sources | — | `_sources.yml` |
| Bronze | `bronze_{entity}.sql` | `_bronze__schema.yml` |
| Silver | `silver_{entity}.sql` | `_silver__schema.yml` |
| Gold | `gold_{entity_or_metric}.sql` | `_gold__schema.yml` |
| Tests | `assert_{description}.sql` | — |
| Analyses | `{descriptive_name}.sql` | — |

---

## Common Patterns

### Deduplication (Silver)

```sql
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
```

### Getting Latest Record Per Symbol

```sql
max_by(current_price, extracted_at)  as current_price,
max(extracted_at)                    as extracted_at
```

Prefer `max_by()` over self-joins or subqueries when you need the value at the latest timestamp.

### Date Filtering for "Today"

```sql
where extracted_at >= date_trunc('day', current_date())
  and extracted_at <  date_trunc('day', current_date() + interval 1 day)
```

### Rolling Window (N Days)

```sql
where extracted_at >= date_trunc('day', current_date() - interval 21 day)
```

---

## Things to Avoid

| ❌ Don't | ✅ Do Instead |
|----------|--------------|
| Hardcode catalog/schema names | Use `{{ ref('model') }}` or `{{ source('src', 'tbl') }}` |
| Use `SELECT *` in gold models | Explicitly list columns |
| Divide without null guard | Wrap with `nullif(denominator, 0)` |
| Skip deduplication in silver | Always deduplicate on natural key + `_file_modified_at` |
| Use `partition_by` on high-cardinality columns | Use `ZORDER BY` via `post_hook` |
| Put business logic in bronze | Bronze is raw reads only |
| Use uppercase SQL keywords | Use lowercase (`select`, `from`, `where`) |
| Use tabs for indentation | Use 2 spaces |
| Name files generically (`model1.sql`) | Use `{layer}_{descriptive_name}.sql` |
| Add trailing commas after the last CTE | End last CTE with `)` then a blank line before `select` |
| Return unrounded calculated columns | Always `round()` numeric outputs |
| Join large tables before filtering | Filter/aggregate in CTEs first, then join |
| Use deprecated `tests:` in YAML | Use `data_tests:` |

---

## Running the Project

```bash
# Parse and validate (no warehouse needed)
uv run dbt parse

# Full run
uv run dbt run

# Run a single model
uv run dbt run -s silver_hourly_prices

# Run with full refresh (rebuilds incremental tables)
uv run dbt run -s silver_hourly_prices --full-refresh

# Run tests
uv run dbt test

# Run tests for a single model
uv run dbt test -s silver_hourly_prices

# Deploy via Databricks Asset Bundles
databricks bundle deploy --target dev
databricks bundle deploy --target prod
```

---

## Adding a New Model Checklist

1. Create `src/models/{layer}/{layer}_{name}.sql` with config block
2. Add model entry to `src/models/{layer}/_{layer}__schema.yml`
3. Include `description` for the model and all columns
4. Add `data_tests` for key columns (`not_null`, `unique`, `accepted_values`)
5. Run `uv run dbt parse` to validate syntax
6. Run `uv run dbt run -s {model_name}` to test execution
7. Run `uv run dbt test -s {model_name}` to verify tests pass
