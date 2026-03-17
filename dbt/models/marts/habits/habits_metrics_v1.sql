-- ============================================================================
-- MART LAYER: Habits Metrics
-- ============================================================================
-- Purpose: Calculate habit completion rates per reporting period and compare
--          to targets defined in the Notion habit reference table.
--
-- Grain: One row per habit per reporting period (week or month)
-- Logic:
--   - Daily habits aggregated to weekly completion rate
--   - Weekly tickbox/number habits: one row per week
--   - HubSpot count habits (met_1to1, met_group): count occurrences per week,
--     check against threshold from habit_reference
--   - Monthly habits: one row per month
-- ============================================================================

with habits as (

    select
        habit_key,
        source_id,
        habit_date,
        habit_period,
        habit,
        is_complete
    from {{ ref('habits_v1') }}
    where is_complete is not null

),

habit_ref as (

    select
        habit_key,
        habit_name,
        category,
        frequency,
        habit_type,
        source,
        target_pct,
        threshold,
        below_threshold,
        active
    from {{ ref('stg_notion__habit_reference') }}

),

-- Daily habits: aggregate to week
daily_by_week as (

    select
        habit,
        {{ trunc_date('week', 'habit_date') }} as period_start,
        'week' as report_period,
        count(*) as total_periods,
        sum(case when is_complete then 1 else 0 end) as completed_periods
    from habits
    where habit_period = 'day'
    group by habit, {{ trunc_date('week', 'habit_date') }}

),

-- Weekly tickbox + number habits (excluding HubSpot count habits)
weekly_by_week as (

    select
        habit,
        habit_date as period_start,
        'week' as report_period,
        1 as total_periods,
        case when is_complete then 1 else 0 end as completed_periods
    from habits
    where habit_period = 'week'
      and habit not in ('met_1to1', 'met_group')

),

-- HubSpot community habits: count engagements per week, check vs. threshold
community_counts as (

    select
        habit,
        habit_date as period_start,
        count(*) as engagement_count
    from habits
    where habit in ('met_1to1', 'met_group')
    group by habit, habit_date

),

community_by_week as (

    select
        c.habit,
        c.period_start,
        'week' as report_period,
        1 as total_periods,
        case
            when c.engagement_count >= coalesce(hr.threshold, 1) then 1
            else 0
        end as completed_periods
    from community_counts c
    left join habit_ref hr on c.habit = hr.habit_key

),

-- Monthly habits: one row per month
monthly_by_month as (

    select
        habit,
        habit_date as period_start,
        'month' as report_period,
        1 as total_periods,
        case when is_complete then 1 else 0 end as completed_periods
    from habits
    where habit_period = 'month'

),

all_periods as (

    select * from daily_by_week
    union all
    select * from weekly_by_week
    union all
    select * from community_by_week
    union all
    select * from monthly_by_month

)

select
    ap.habit,
    hr.habit_name,
    hr.category,
    hr.frequency,
    hr.source,
    hr.habit_type,
    ap.period_start,
    ap.report_period,
    ap.total_periods,
    ap.completed_periods,
    round(
        cast(ap.completed_periods as double) / nullif(ap.total_periods, 0),
        4
    ) as completion_rate,
    hr.target_pct,
    round(
        cast(ap.completed_periods as double) / nullif(ap.total_periods, 0),
        4
    ) >= hr.target_pct as target_met,
    hr.threshold,
    hr.below_threshold,
    hr.active
from all_periods ap
left join habit_ref hr on ap.habit = hr.habit_key
