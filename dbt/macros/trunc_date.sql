-- Adapter-aware macro to normalize date_trunc across BigQuery and DuckDB
-- Usage: {{ trunc_date('\'month\'', 'my_date_column') }}
-- Note: pass the `part` argument as a quoted literal (e.g. 'month', 'day', 'year')

{% macro trunc_date(part, column_name) -%}
    {{ return(adapter.dispatch('trunc_date')(part, column_name)) }}
{%- endmacro %}

{% macro bigquery__trunc_date(part, column_name) -%}
    {# Remove surrounding quotes if they exist, then uppercase to match BigQuery PART token (MONTH, YEAR, etc.) #}
    {% set _part = part.replace("'", "").replace('"', '') | upper %}
    date_trunc({{ column_name }}, {{ _part }})
{%- endmacro %}

{% macro duckdb__trunc_date(part, column_name) -%}
    {# DuckDB expects date_trunc('<part>', timestamp) with the part as a string (lowercase is OK) #}
    {% set _part = part.replace("'", "").replace('"', '') | lower %}
    date_trunc('{{ _part }}', {{ column_name }})
{%- endmacro %}
