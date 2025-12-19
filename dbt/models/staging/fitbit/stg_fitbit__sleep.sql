-- ============================================================================
-- STAGING LAYER: Fitbit Sleep
-- ============================================================================
-- Purpose: Clean and standardize raw Fitbit sleep data. Calculates sleep duration
--          metrics, parses timestamps, and determines if sleep goals were met.
--
-- Source: fitbit.sleep (raw dlt-loaded data)
-- Transformations:
--   - Cast date_of_sleep to date type
--   - Calculate duration in hours (duration_ms / 3600000)
--   - Determine if sleep goal was met (duration >= sleep_goal variable)
--   - Deduplicate by log_id (keep most recent)
-- Output: One row per unique sleep log with duration metrics and goal status
-- ============================================================================

with sleep as (
    -- CTE: Sleep Raw Extraction and Calculations
    -- Purpose: Extract and transform raw Fitbit sleep data into clean, typed columns.
    --          Calculates duration metrics and determines if sleep goals were met.
    -- Source: fitbit.sleep (raw API data loaded by dlt)
    -- Transformations:
    --   - Cast date_of_sleep to date type
    --   - Calculate duration in hours (duration_ms / 3600000 milliseconds)
    --   - Compare duration to sleep_goal variable (default: 7 hours = 25,200,000 ms)
    --   - Rename columns: start_time -> started_at, end_time -> ended_at

    select
        log_id,  -- Fitbit sleep log ID
        cast(date_of_sleep as date) as date_of_sleep,  -- Date of sleep (not when logged)
        duration as duration_ms,  -- Sleep duration in milliseconds
        duration / 3600000 as duration_hr,  -- Sleep duration in hours (calculated)
        start_time as started_at,  -- Timestamp when sleep started
        end_time as ended_at,  -- Timestamp when sleep ended
        type as sleep_type,  -- Type of sleep (classic, stages)
        log_type,  -- Log type classification
        -- Check if sleep duration met or exceeded the goal
        -- sleep_goal variable: default 25,200,000 ms = 7 hours
        duration >= {{ var('sleep_goal') }} as sleep_goal_met
    from
        {{ make_source('fitbit', 'sleep') }}

),

unique_sleep as (
    -- CTE: Deduplicated Sleep Logs
    -- Purpose: Remove duplicate records for the same log_id, keeping only
    --          the most recent version (by date_of_sleep).
    -- Method: Uses dbt_utils.deduplicate macro with ROW_NUMBER() window function
    -- Output: One row per unique log_id (most recent version)

    {{ dbt_utils.deduplicate(
        relation='sleep',  -- Source CTE to deduplicate
        partition_by='log_id',  -- Group duplicates by log_id
        order_by='date_of_sleep desc',  -- Keep record with latest date_of_sleep
        )
    }}

)

-- Final output: Clean, deduplicated sleep data with duration metrics and goal status
select * from unique_sleep
