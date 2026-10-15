{% macro source_volume_path(source_name, table_name) %}
  {#- Registers the source() dependency for lineage; the relation itself is unused
      because the raw data is JSON files in a Volume, not a table. -#}
  {%- set _ = source(source_name, table_name) -%}
  {%- if execute -%}
    {%- set node = graph.sources.get('source.' ~ project_name ~ '.' ~ source_name ~ '.' ~ table_name) -%}
    {%- set path = node.config.meta.get('volume_path') or node.meta.get('volume_path') -%}
    {%- if not path -%}
      {{ exceptions.raise_compiler_error("No meta.volume_path set for source " ~ source_name ~ "." ~ table_name) }}
    {%- endif -%}
    {{- path -}}
  {%- else -%}
    {{- '' -}}
  {%- endif -%}
{% endmacro %}
