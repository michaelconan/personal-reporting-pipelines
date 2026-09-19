-- ============================================================================
-- CORE LAYER: Sleep Sessions
-- ============================================================================
-- Purpose: Source-agnostic sleep session entity. One row per sleep session
--          regardless of which wearable/API recorded it, so downstream
--          models never depend on a specific provider.
--
--          Overlapping sessions (duplicates of the same night recorded under
--          different data points, e.g. CLASSIC + STAGES, or unclassified
--          splits of a main sleep) are resolved to their single most robust
--          representative: a stage-detailed record wins, then a main sleep
--          over a nap, then the longer interval.
--
-- Source: stg_google_health (currently Google Health; future providers map here)
-- Output: One row per sleep session
-- ============================================================================

with stg_google_health__sleep as (

    select * from {{ ref('stg_google_health__sleep') }}

),

sleep_sessions as (
-- Assign a robustness priority so overlaps resolve towards the record that
-- carries the most information.
    select
        sleep_id as session_id,
        sleep_date as session_date,
        started_at,
        ended_at,
        sleep_minutes as duration_minutes,
        asleep_minutes,
        sleep_type,
        is_main_sleep,
        is_nap,
        'google_health' as provider,
        case
            when sleep_type = 'STAGES' then 3
            when is_main_sleep then 2
            when is_nap then 1
            else 0
        end as overlap_priority
    from
        stg_google_health__sleep

),

final as (
-- Drop any session that overlaps a strictly more robust one. Overlaps chain
-- into connected clusters, so this keeps exactly the one most robust record
-- per cluster (ties break on longer duration, then lexicographic session id).
-- Implemented as a LEFT JOIN + null filter (a correlated NOT EXISTS with range
-- predicates cannot be expressed as an anti-semi join on BigQuery).
    select
        this.session_id,
        this.session_date,
        this.started_at,
        this.ended_at,
        this.duration_minutes,
        this.asleep_minutes,
        this.sleep_type,
        this.is_main_sleep,
        this.is_nap,
        this.provider
    from
        sleep_sessions as this
    left join sleep_sessions as other
        on other.session_id <> this.session_id
        and other.started_at < this.ended_at
        and this.started_at < other.ended_at
        and (
            other.overlap_priority > this.overlap_priority
            or (
                other.overlap_priority = this.overlap_priority
                and other.duration_minutes > this.duration_minutes
            )
            or (
                other.overlap_priority = this.overlap_priority
                and other.duration_minutes = this.duration_minutes
                and other.session_id > this.session_id
            )
        )
    where
        other.session_id is null
)

select * from final