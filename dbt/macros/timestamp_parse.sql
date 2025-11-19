-- Adapter-aware macro to parse timestamps from columns
-- Usage: {{ timestamp_parse('my_column') }}
--
{% macro timestamp_parse(column) -%}
    {{ return(adapter.dispatch('timestamp_parse')(column)) }}
{%- endmacro %}

{% macro bigquery__timestamp_parse(column) -%}
    timestamp_millis({{ column }})
{%- endmacro %}

{% macro duckdb__timestamp_parse(column) -%}
    make_timestamp(cast(column as bigint) * 1000000)
{%- endmacro %}