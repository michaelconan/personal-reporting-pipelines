-- Adapter-aware macro to unnest JSON arrays in a cross-database compatible way
-- Works with both BigQuery and DuckDB using adapter dispatch pattern
-- Usage: {{ unnest_json_array('column_name', 'alias_name') }}
--
-- Example:
--   from table,
--   {{ unnest_json_array('contact_ids', 'contact_id') }}

{% macro unnest_json_array(column_name, alias_name) -%}
    {{ return(adapter.dispatch('unnest_json_array')(column_name, alias_name)) }}
{%- endmacro %}

{% macro bigquery__unnest_json_array(column_name, alias_name) -%}
    unnest(json_value_array({{ column_name }})) as {{ alias_name }}
{%- endmacro %}

{% macro duckdb__unnest_json_array(column_name, alias_name) -%}
    unnest(cast(json_extract({{ column_name }}, '$') as varchar[])) as t({{ alias_name }})
{%- endmacro %}
