-- Macro to extract JSON scalar values in a cross-database compatible way
-- Works with both BigQuery and DuckDB

{% macro json_extract_value(column_name, json_path) %}
    {% if target.type == 'bigquery' %}
    json_extract_scalar({{ column_name }}, {{ json_path }})
  {% elif target.type == 'duckdb' %}
    json_extract_string({{ column_name }}, {{ json_path }})
  {% else %}
        {{ exceptions.raise_compiler_error("Unsupported target type: " ~ target.type ~ ". This macro only supports 'bigquery' and 'duckdb'.") }}
    {% endif %}
{% endmacro %}
