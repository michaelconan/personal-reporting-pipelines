-- Override of dbt_utils.deduplicate for duckdb, see:
-- https://docs.getdbt.com/reference/dbt-jinja-functions/dispatch#overriding-package-macros
-- https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#dispatch-macros

{% macro duckdb__deduplicate(relation, partition_by, order_by) -%}

    select *
    from {{ relation }} as tt
    qualify
        row_number() over (
            partition by {{ partition_by }}
            order by {{ order_by }}
        ) = 1

{% endmacro %}