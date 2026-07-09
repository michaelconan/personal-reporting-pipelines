-- Macro to extract the plain text from Notion's Rich Text / Title JSON array
-- Works with both BigQuery and DuckDB using adapter dispatch pattern

{% macro extract_notion_title(column_name) -%}
    {{ return(adapter.dispatch('extract_notion_title')(column_name)) }}
{%- endmacro %}

{% macro bigquery__extract_notion_title(column_name) -%}
    json_extract_scalar({{ column_name }}, '$[0].plain_text')
{%- endmacro %}

{% macro duckdb__extract_notion_title(column_name) -%}
    json_extract_string(replace({{ column_name }}, '""', '"'), '/0/plain_text')
{%- endmacro %}
