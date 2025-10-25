-- Override of dbt_utils.deduplicate for duckdb, see:
-- https://docs.getdbt.com/reference/dbt-jinja-functions/dispatch#overriding-package-macros

{% macro deduplicate(relation, partition_by, order_by) -%}
    {{ return(adapter.dispatch('deduplicate')(relation, partition_by, order_by)) }}
{%- endmacro %}

{% macro default__deduplicate(relation, partition_by, order_by) -%}

    {{ return(dbt_utils.deduplicate(relation, partition_by, order_by)) }}

{%- endmacro %}

{% macro duckdb__deduplicate(relation, partition_by, order_by) -%}

    select *
    from {{ relation }} as tt
    qualify
        row_number() over (
            partition by {{ partition_by }}
            order by {{ order_by }}
        ) = 1

{% endmacro %}