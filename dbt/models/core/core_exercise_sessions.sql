-- ============================================================================
-- CORE LAYER: Exercise Sessions
-- ============================================================================
-- Purpose: Conformed exercise entity. One row per exercise session regardless
--          of which wearable/API recorded it, so downstream models never
--          depend on a specific provider.
--
--          Overlapping sessions (the same activity recorded twice, e.g. a
--          short partly-nested point underneath the full session) are deduped
--          to the longest interval so the most complete record survives.
--
-- Source: stg_google_health__exercise
-- Output: One row per exercise session
-- ============================================================================

with stg_google_health__exercise as (

    select * from {{ ref('stg_google_health__exercise') }}

),

exercise_sessions as (

    select
        exercise_id as session_id,
        started_at,
        ended_at,
        duration_seconds,
        duration_minutes,
        exercise_type,
        activity_name,
        step_count,
        calories_kcal,
        distance_meters,
        pace_seconds_per_meter,
        {{ seconds_between('started_at', 'ended_at') }} as interval_seconds
    from
        stg_google_health__exercise

),

final as (
-- Drop any session that overlaps a longer one; ties break on the larger
-- session id so the choice is deterministic. Implemented as a LEFT JOIN +
-- null filter (a correlated NOT EXISTS with range predicates cannot be
-- expressed as an anti-semi join on BigQuery).
    select
        this.session_id,
        this.started_at,
        this.ended_at,
        this.duration_minutes as active_duration_minutes,
        this.duration_seconds as active_duration_seconds,
        this.exercise_type,
        this.activity_name,
        this.step_count,
        this.calories_kcal,
        this.distance_meters,
        this.pace_seconds_per_meter,
        'google_health' as provider
    from
        exercise_sessions as this
    left join exercise_sessions as other
        on other.session_id <> this.session_id
        and other.started_at < this.ended_at
        and this.started_at < other.ended_at
        and (
            other.interval_seconds > this.interval_seconds
            or (
                other.interval_seconds = this.interval_seconds
                and other.session_id > this.session_id
            )
        )
    where
        other.session_id is null
)

select * from final