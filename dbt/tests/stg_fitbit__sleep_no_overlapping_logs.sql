-- Singular test: fails if any sleep logs have overlapping time periods.
-- Uses LEAD() instead of a self-join to avoid a DuckDB internal assertion
-- failure (TIMESTAMP != VARCHAR in ColumnBindingResolver) that occurs when a
-- view using dbt_utils.deduplicate is referenced twice in the same query.
-- Mathematical note: if any two intervals overlap, an adjacent pair (when
-- sorted by start time) must also overlap, so LEAD on adjacent rows is
-- sufficient to detect any overlap.
with sleep_typed as (
    select
        cast(start_time as timestamp) as sleep_started_at,
        cast(end_time as timestamp) as sleep_ended_at
    from {{ make_source('fitbit', 'sleep') }}
),
sleep_ordered as (
    select
        sleep_started_at,
        sleep_ended_at,
        lead(sleep_started_at) over (
            order by sleep_started_at
        ) as next_sleep_started_at
    from sleep_typed
)
select 1
from sleep_ordered
where next_sleep_started_at is not null
  and sleep_ended_at > next_sleep_started_at
