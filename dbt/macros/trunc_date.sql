-- Adapter-aware macro to normalize date_trunc across BigQuery and DuckDB
-- Usage: {{ trunc_date('\'month\'', 'my_date_column') }}
-- Note: pass the `part` argument as a quoted literal (e.g. 'month', 'day', 'year')

{% macro trunc_date(part, column) -%}
    {{ return(adapter.dispatch('trunc_date')(part, column)) }}
{%- endmacro %}

{% macro bigquery__trunc_date(part, column) -%}
    {# Remove surrounding quotes if they exist, then uppercase to match BigQuery PART token (MONTH, YEAR, etc.) #}
    {% set _part = part.replace("'", "").replace('"', '') | upper %}
    date_trunc({{ column }}, {{ _part }})
{%- endmacro %}

{% macro duckdb__trunc_date(part, column) -%}
    {# DuckDB expects date_trunc('<part>', timestamp) with the part as a string (lowercase is OK) #}
    {% set _part = part.replace("'", "").replace('"', '') | lower %}
    date_trunc('{{ _part }}', {{ column }})
{%- endmacro %}
