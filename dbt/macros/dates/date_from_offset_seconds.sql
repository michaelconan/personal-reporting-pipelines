-- Adapter-aware macro to derive a local date from a UTC timestamp plus a
-- second-based UTC offset string (e.g. '3600s', '0s', '-28800s').
-- Usage: {{ date_from_offset_seconds('timestamp_col', 'offset_col') }}

{% macro date_from_offset_seconds(timestamp_expr, offset_seconds_expr) -%}
    {{ return(adapter.dispatch('date_from_offset_seconds')(timestamp_expr, offset_seconds_expr)) }}
{%- endmacro %}

{% macro bigquery__date_from_offset_seconds(timestamp_expr, offset_seconds_expr) -%}
    date(
        timestamp_add(
            {{ timestamp_expr }},
            interval coalesce(safe_cast(replace({{ offset_seconds_expr }}, 's', '') as int64), 0) second
        )
    )
{%- endmacro %}

{% macro duckdb__date_from_offset_seconds(timestamp_expr, offset_seconds_expr) -%}
    cast(
        {{ timestamp_expr }}
        + to_seconds(coalesce(try_cast(replace({{ offset_seconds_expr }}, 's', '') as integer), 0))
        as date
    )
{%- endmacro %}

{% macro databricks__date_from_offset_seconds(timestamp_expr, offset_seconds_expr) -%}
    date(
        {{ timestamp_expr }} + 
        make_interval(0, 0, 0, 0, 0, 0, coalesce(cast(replace({{ offset_seconds_expr }}, 's', '') as int), 0))
    )
{%- endmacro %}
