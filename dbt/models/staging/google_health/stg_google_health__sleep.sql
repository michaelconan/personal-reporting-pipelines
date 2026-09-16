-- ============================================================================
-- STAGING LAYER: Google Health Sleep
-- ============================================================================
-- Purpose: Clean and standardize the raw Google Health (formerly Health Connect/
--          Fitbit-derived) sleep session data under the new layered structure.
--
-- Source: google_health.sleep (raw dlt-loaded data)
-- Transformations:
--   - Assign a session identifier from the raw data point name
--   - Attribute the session to the civil (local) date it starts
--   - Calculate sleep duration in minutes from the session interval
--   - Rename columns toward the source-agnostic core contract
-- Output: One row per unique sleep session
-- ============================================================================

with sleep_sessions as (

    select
        name as sleep_id,  -- Google Health / Fitbit sleep data point identifier
        {{ date_from_offset_seconds('sleep__interval__start_time', 'sleep__interval__start_utc_offset') }}
            as sleep_date,
        sleep__interval__start_time as started_at,  -- Timestamp when sleep started
        sleep__interval__end_time as ended_at,  -- Timestamp when sleep ended
        round({{ seconds_between('sleep__interval__start_time', 'sleep__interval__end_time') }} / 60.0, 1)
            as sleep_minutes,
        -- Minutes recorded asleep per summary
        {{ cast_safe('sleep__summary__minutes_asleep', 'integer') }} as asleep_minutes,
        sleep__type as sleep_type,  -- Recording format (CLASSIC, STAGES, ...)
        sleep__metadata__main_sleep as is_main_sleep,  -- Whether this is the main sleep of the night
        sleep__metadata__nap as is_nap,  -- Whether this session is a nap
        coalesce(sleep__update_time, sleep__create_time) as updated_at
    from
        {{ make_source('google_health', 'sleep') }}

),

unique_sleep_sessions as (
-- CTE: Deduplicated Sleep Sessions
-- Purpose: Remove duplicate records for the same data point, keeping only
--          the most recently updated version.
    {{ dbt_utils.deduplicate(
        relation='sleep_sessions',
        partition_by='sleep_id',
        order_by='updated_at desc'
    ) }}
)

-- Final output: Clean, deduplicated sleep session data
select * from unique_sleep_sessions
