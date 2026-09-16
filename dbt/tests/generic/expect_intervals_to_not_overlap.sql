{#
    Generic test: fails when any [start, end) intervals in the model overlap.

    An adjacent-pair check via LEAD is sufficient: if any two intervals overlap,
    an adjacent pair (when sorted by start time) must also overlap. This avoids
    an O(n^2) self-join, which also sidesteps a DuckDB internal assertion
    failure (TIMESTAMP != VARCHAR in ColumnBindingResolver) seen when a view
    using dbt_utils.deduplicate is referenced twice in the same query.

    Optional group_by partitions the window function so intervals that belong
    to different groups (e.g. devices) may legitimately overlap within a
    period.
#}
{% test expect_intervals_to_not_overlap(model, start_column_name, end_column_name, group_by=None) %}

with ordered_intervals as (

    select
        {{ start_column_name }} as interval_start,
        {{ end_column_name }} as interval_end,
        lead({{ start_column_name }}) over (
            {%- if group_by %} partition by {{ group_by }}{% endif %}
            order by {{ start_column_name }}
        ) as next_interval_start
    from
        {{ model }}

)

select
    interval_start,
    interval_end,
    next_interval_start
from
    ordered_intervals
where
    next_interval_start is not null
    and interval_end > next_interval_start

{% endtest %}
