-- ============================================================================
-- CORE LAYER: Daily Steps
-- ============================================================================
-- Purpose: Conformed daily physical activity entity. One row per local day
--          with total steps, aggregated from provider step intervals. The
--          grain matches any comparable wearable data source.
--
-- Source: stg_google_health__steps (interval grain)
-- Output: One row per local activity date
-- ============================================================================

with step_intervals as (

    select
        activity_date,
        step_count
    from
        {{ ref('stg_google_health__steps') }}

),

daily_step_totals as (

    select
        activity_date,
        cast(sum(step_count) as bigint) as step_count,
        count(*) as interval_count
    from
        step_intervals
    group by
        activity_date

)

select
    activity_date,
    step_count,
    interval_count,
    'google_health' as provider
from
    daily_step_totals
