-- Adapter-aware macro to build a date from separate year/month/day columns
-- Usage: {{ date_from_parts('col_year', 'col_month', 'col_day') }}

{% macro date_from_parts(year_expr, month_expr, day_expr) -%}
    {{ return(adapter.dispatch('date_from_parts')(year_expr, month_expr, day_expr)) }}
{%- endmacro %}

{% macro bigquery__date_from_parts(year_expr, month_expr, day_expr) -%}
    date({{ year_expr }}, {{ month_expr }}, {{ day_expr }})
{%- endmacro %}

{% macro duckdb__date_from_parts(year_expr, month_expr, day_expr) -%}
    make_date({{ year_expr }}, {{ month_expr }}, {{ day_expr }})
{%- endmacro %}
