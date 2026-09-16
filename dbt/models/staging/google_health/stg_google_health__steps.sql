-- ============================================================================
-- STAGING LAYER: Google Health Steps
-- ============================================================================
-- Purpose: Clean and standardize the raw Google Health step interval data.
--          Steps stay at interval grain in staging; the per-day aggregation
--          that the habits pipeline consumes lives in the core layer.
--
-- Source: google_health.steps (raw dlt-loaded data)
-- Transformations:
--   - Cast the step count to an integer
--   - Derive the local (civil) activity date from the raw civil date columns
--   - Rename columns toward the source-agnostic core contract
-- Output: One row per step interval data point
-- ============================================================================

with step_intervals as (

    select
        steps__interval__start_time as started_at,  -- Start of the step interval
        steps__interval__end_time as ended_at,  -- End of the step interval
        {{ date_from_parts(
            'steps__interval__civil_start_time__date__year',
            'steps__interval__civil_start_time__date__month',
            'steps__interval__civil_start_time__date__day'
        ) }} as activity_date,  -- Local (civil) date the interval belongs to
        {{ cast_safe('steps__count', 'integer') }} as steps,  -- Steps recorded within the interval
        steps__interval__start_utc_offset,
        data_source__platform as data_source_platform,
        data_source__device__display_name as device_name,
        _dlt_id  -- dlt-generated row hash (row key)
    from
        {{ make_source('google_health', 'steps') }}

)

-- Final output: Typed step interval data
select * from step_intervals
