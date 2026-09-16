-- ============================================================================
-- CORE LAYER: Exercise Sessions
-- ============================================================================
-- Purpose: Conformed exercise entity. One row per exercise session regardless
--          of which wearable/API recorded it, so downstream models never
--          depend on a specific provider.
--
-- Source: stg_google_health__exercise
-- Output: One row per exercise session
-- ============================================================================

select
    exercise_id as session_id,
    started_at,
    ended_at,
    duration_minutes as active_duration_minutes,
    duration_seconds as active_duration_seconds,
    exercise_type,
    activity_name,
    steps,
    calories_kcal,
    distance_meters,
    pace_seconds_per_meter,
    'google_health' as provider
from
    {{ ref('stg_google_health__exercise') }}
