{% macro weekly_optimize() %}
  {% do run_query("OPTIMIZE " ~ ref('silver_hourly_prices') ~ " ZORDER BY (extracted_at, symbol)") %}
{% endmacro %}
