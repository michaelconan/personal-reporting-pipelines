-- Adapter-aware macro to compute the number of seconds between two timestamps
-- Usage: {{ seconds_between('start_ts_col', 'end_ts_col') }}

{% macro seconds_between(start_expr, end_expr) -%}
    {{ return(adapter.dispatch('seconds_between')(start_expr, end_expr)) }}
{%- endmacro %}

{% macro bigquery__seconds_between(start_expr, end_expr) -%}
    timestamp_diff({{ end_expr }}, {{ start_expr }}, second)
{%- endmacro %}

{% macro duckdb__seconds_between(start_expr, end_expr) -%}
    date_diff('second', {{ start_expr }}, {{ end_expr }})
{%- endmacro %}

{% macro databricks__seconds_between(start_expr, end_expr) -%}
    (unix_timestamp({{ end_expr }}) - unix_timestamp({{ start_expr }}))
{%- endmacro %}
