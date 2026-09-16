-- Macro to extract and concatenate the plain text from Notion's Rich Text / Title JSON array
-- Works with both BigQuery and DuckDB using adapter dispatch pattern

{% macro extract_notion_title(column_name) -%}
    {{ return(adapter.dispatch('extract_notion_title')(column_name)) }}
{%- endmacro %}

{% macro bigquery__extract_notion_title(column_name) -%}
    (select string_agg(json_extract_scalar(item, '$.plain_text'), '') from unnest(json_extract_array({{ column_name }})) as item)
{%- endmacro %}

{% macro duckdb__extract_notion_title(column_name) -%}
    list_aggregate(list_transform(json_transform({{ column_name }}, '[{"plain_text": "VARCHAR"}]'), x -> x.plain_text), 'string_agg', '')
{%- endmacro %}
