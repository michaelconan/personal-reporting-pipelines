-- Override of dbt_utils.deduplicate for duckdb, see:
-- https://docs.getdbt.com/reference/dbt-jinja-functions/dispatch#overriding-package-macros
-- https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#dispatch-macros
--
-- Uses DISTINCT ON instead of QUALIFY row_number() to work around a DuckDB bug
-- (tested on 1.5.1) where window functions inside CTEs cause incorrect type
-- binding for CAST(... AS DATE) columns when those CTEs are used inside views
-- that are subsequently queried via UNPIVOT + UNION ALL.
{% macro duckdb__deduplicate(relation, partition_by, order_by) -%}

    select distinct on ({{ partition_by }}) *
    from {{ relation }}
    order by {{ partition_by }}, {{ order_by }}

{% endmacro %}
