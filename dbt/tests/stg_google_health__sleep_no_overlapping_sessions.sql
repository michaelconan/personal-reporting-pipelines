-- Singular test: fails if any sleep sessions have overlapping time periods.
-- Uses LEAD() instead of a self-join to avoid a DuckDB internal assertion
-- failure (TIMESTAMP != VARCHAR in ColumnBindingResolver) that occurs when a
-- view using dbt_utils.deduplicate is referenced twice in the same query.
-- Mathematical note: if any two intervals overlap, an adjacent pair (when
-- sorted by start time) must also overlap, so LEAD on adjacent rows is
-- sufficient to detect any overlap.
with sleep_typed as (
    select
        sleep__interval__start_time as session_started_at,
        sleep__interval__end_time as session_ended_at
    from {{ make_source('google_health', 'sleep') }}
),

sleep_ordered as (
    select
        session_started_at,
        session_ended_at,
        lead(session_started_at) over (
            order by session_started_at
        ) as next_session_started_at
    from sleep_typed
)

select 1
from sleep_ordered
where
    next_session_started_at is not null
    and session_ended_at > next_session_started_at
