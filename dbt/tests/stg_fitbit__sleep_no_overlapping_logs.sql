-- Singular test: fails if any sleep logs have overlapping time periods.
-- Uses LEAD() instead of a self-join to avoid a DuckDB internal assertion
-- failure (TIMESTAMP != VARCHAR in ColumnBindingResolver) that occurs when a
-- view using dbt_utils.deduplicate is referenced twice in the same query.
-- Mathematical note: if any two intervals overlap, an adjacent pair (when
-- sorted by start time) must also overlap, so LEAD on adjacent rows is
-- sufficient to detect any overlap.
with sleep_ordered as (
    select
        log_id,
        cast(started_at as timestamp) as started_at,
        cast(ended_at as timestamp) as ended_at,
        lead(cast(started_at as timestamp)) over (
            order by cast(started_at as timestamp)
        ) as next_started_at
    from {{ ref('stg_fitbit__sleep') }}
)
select log_id
from sleep_ordered
where next_started_at is not null
  and ended_at > next_started_at
