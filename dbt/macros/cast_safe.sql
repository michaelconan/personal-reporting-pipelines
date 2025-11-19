-- Adapter-aware macro to safely cast columns to a specified type
-- Usage: {{ cast_safe('my_column', 'INT64') }}
--
{% macro cast_safe(column, api_type) -%}
    {{ return(adapter.dispatch('cast_safe')(column, api_type)) }}
{%- endmacro %}

{% macro bigquery__cast_safe(column, api_type) -%}
    SAFE_CAST({{ column }} AS {{ api_type }})
{%- endmacro %}

{% macro duckdb__cast_safe(column, api_type) -%}
    TRY_CAST({{ column }} AS {{ api_type }})
{%- endmacro %}
