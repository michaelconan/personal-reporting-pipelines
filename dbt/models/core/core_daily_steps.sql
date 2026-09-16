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

with daily_step_totals as (

    select
        activity_date,
        sum(steps) as steps,
        count(*) as interval_count
    from
        {{ ref('stg_google_health__steps') }}
    group by
        activity_date

)

select
    activity_date,
    steps,
    interval_count,
    'google_health' as provider
from
    daily_step_totals
