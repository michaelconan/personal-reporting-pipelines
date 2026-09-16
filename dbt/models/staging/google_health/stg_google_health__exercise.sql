-- ============================================================================
-- STAGING LAYER: Google Health Exercise
-- ============================================================================
-- Purpose: Clean and standardize the raw Google Health exercise session data.
--
-- Source: google_health.exercise (raw dlt-loaded data)
-- Transformations:
--   - Parse the active duration string (e.g. 400.500s) into seconds/minutes
--   - Convert distance millimetres to metres
--   - Rename columns toward the source-agnostic core contract
-- Output: One row per unique exercise data point
-- ============================================================================

with exercise_sessions as (

    select
        name as exercise_id,  -- Google Health exercise data point identifier
        exercise__exercise_type as exercise_type,  -- EXERCISE_TYPE (WALKING, OTHER, ...)
        exercise__display_name as activity_name,  -- Human readable activity name
        exercise__interval__start_time as started_at,  -- Session interval start
        exercise__interval__end_time as ended_at,  -- Session interval end
        -- Active duration parsed from "<n>s"
        {{ cast_safe("replace(exercise__active_duration, 's', '')", 'numeric') }} as duration_seconds,
        -- Active duration in minutes
        {{ cast_safe("replace(exercise__active_duration, 's', '')", 'numeric') }} / 60.0 as duration_minutes,
        {{ cast_safe('exercise__metrics_summary__steps', 'integer') }} as step_count,  -- Steps recorded in the session
        -- Calories burned (kcal)
        {{ cast_safe('exercise__metrics_summary__calories_kcal', 'integer') }} as calories_kcal,
        -- Distance in metres
        {{ cast_safe('exercise__metrics_summary__distance_millimeters', 'integer') }} / 1000.0 as distance_meters,
        exercise__metrics_summary__average_pace_seconds_per_meter as pace_seconds_per_meter,
        exercise__create_time as created_at,
        exercise__update_time as updated_at
    from
        {{ source('google_health', 'exercise') }}

),

unique_exercise_sessions as (
-- CTE: Deduplicated exercise sessions
-- Purpose: The pipeline appends with an inclusive cursor, so repeated versions
--          of an exercise data point can exist across loads. Keep only the
--          most recently updated version of each data point.
    {{ dbt_utils.deduplicate(
        relation='exercise_sessions',
        partition_by='exercise_id',
        order_by='updated_at desc'
    ) }}
)

-- Final output: Clean, deduplicated exercise session data
select * from unique_exercise_sessions
