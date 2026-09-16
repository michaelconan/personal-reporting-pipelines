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
        {{ cast_safe('steps__count', 'integer') }} as step_count,  -- Steps recorded within the interval
        steps__interval__start_utc_offset as start_utc_offset,
        data_source__platform as data_source_platform,
        data_source__device__display_name as device_name,
        _dlt_load_id,  -- dlt load id (recency tie-breaker for dedupe)
        _dlt_id  -- dlt-generated row hash (row key)
    from
        {{ source('google_health', 'steps') }}

),

unique_step_intervals as (
-- CTE: Deduplicated step intervals
-- Purpose: The pipeline appends with an inclusive cursor, so boundary records
--          can arrive in more than one load. The natural key of a step
--          interval is its interval window; keep only the most recent delivery
--          per interval (the load id is only a recency tie-breaker).
    {{ dbt_utils.deduplicate(
        relation='step_intervals',
        partition_by='started_at, ended_at',
        order_by='_dlt_load_id desc'
    ) }}
)

-- Final output: Deduplicated step interval data
select * from unique_step_intervals
