-- Adapter-aware macro to parse timestamps from columns
-- Usage: {{ timestamp_parse('my_column') }}
--
{% macro timestamp_parse(column_name) -%}
    {{ return(adapter.dispatch('timestamp_parse')(column_name)) }}
{%- endmacro %}

{% macro bigquery__timestamp_parse(column_name) -%}
    timestamp_millis({{ column_name }})
{%- endmacro %}

{% macro duckdb__timestamp_parse(column_name) -%}
    make_timestamp(cast({{ column_name }} as bigint) * 1000000)
{%- endmacro %}