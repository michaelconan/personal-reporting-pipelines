-- Macro to extract JSON scalar values in a cross-database compatible way
-- Works with both BigQuery and DuckDB using adapter dispatch pattern

{% macro json_extract_value(column_name, json_path) -%}
    {{ return(adapter.dispatch('json_extract_value')(column_name, json_path)) }}
{%- endmacro %}

{% macro bigquery__json_extract_value(column_name, json_path) -%}
    json_extract_scalar({{ column_name }}, {{ json_path }})
{%- endmacro %}

{% macro duckdb__json_extract_value(column_name, json_path) -%}
    json_extract_string({{ column_name }}, {{ json_path }})
{%- endmacro %}
