-- ============================================================================
-- STAGING LAYER: Fitbit Activities
-- ============================================================================
-- Purpose: Clean and standardize raw Fitbit activity data. Aggregates daily
--          steps across activity logs and determines if step goals were met.
--
-- Source: fitbit.activities (raw dlt-loaded data)
-- Transformations:
--   - Parse date from start_time
--   - Aggregate steps per day
--   - Determine if step goal met (steps >= steps_goal variable)
-- Output: One row per day with total steps and goal status
-- ============================================================================

with activities as (

    select
        cast(left(cast(start_time as varchar), 10) as date) as activity_date,
        coalesce(steps, 0) as steps
    from
        {{ make_source('fitbit', 'activities') }}

),

daily_steps as (

    select
        activity_date,
        sum(steps) as total_steps
    from activities
    group by activity_date

)

select
    activity_date as date_of_activity,
    total_steps as steps,
    total_steps >= {{ var('steps_goal') }} as steps_goal_met
from daily_steps
