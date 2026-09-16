-- ============================================================================
-- MART LAYER: Habits Metrics
-- ============================================================================
-- Purpose: Completion rates for every habit per tracked period against the
--          Notion habit reference targets and thresholds. Grain: one row per
--          habit per tracked period (metric_key).
--
-- Sources: habits (unified habit occurrences),
--          stg_notion__habit_reference (targets + thresholds)
-- Output: One row per habit per tracked period
-- ============================================================================

-- Import CTEs: one per upstream reference, selected as-is
with habits as (

    select
        habit_key,
        source_id,
        habit_date,
        habit_period,
        habit,
        habit_value
    from {{ ref('habits', v=1) }}
    where habit_value is not null

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
        is_below_threshold,
        is_active
    from {{ ref('stg_notion__habit_reference') }}

),

-- Transform CTEs: resolve is_complete per occurrence against the reference
-- tickbox: 1.0 = done
-- number (above threshold): habit_value >= threshold
-- number (below threshold): habit_value <= threshold
-- count (HubSpot met_*): aggregated separately below
habit_occurrences as (

    select
        h.habit,
        h.habit_date,
        h.habit_period,
        h.habit_value,
        hr.habit_type,
        hr.threshold,
        hr.is_below_threshold,
        hr.target_pct,
        hr.habit_name,
        hr.category,
        hr.frequency,
        hr.source,
        hr.is_active,
        case
            when hr.habit_type = 'tickbox'
                then h.habit_value = 1.0
            when hr.habit_type = 'number' and not hr.is_below_threshold
                then h.habit_value >= hr.threshold
            when hr.habit_type = 'number' and hr.is_below_threshold
                then h.habit_value <= hr.threshold
        end as is_complete
    from habits as h
    left join habit_ref as hr on h.habit = hr.habit_key

),

-- Daily tickbox + number habits: aggregate each calendar week
daily_by_week as (

    select
        habit,
        cast({{ trunc_date('week', 'habit_date') }} as date) as period_start,
        'week' as report_period,
        cast(count(*) as bigint) as total_periods,
        cast(sum(case when is_complete then 1 else 0 end) as bigint) as completed_periods
    from habit_occurrences
    where
        habit_period = 'day'
        and habit_type in ('tickbox', 'number')
    group by habit, {{ trunc_date('week', 'habit_date') }}

),

-- Weekly tickbox + number habits: one row per week, already at correct grain
weekly_by_week as (

    select
        habit,
        cast(habit_date as date) as period_start,
        'week' as report_period,
        cast(1 as bigint) as total_periods,
        cast(case when is_complete then 1 else 0 end as bigint) as completed_periods
    from habit_occurrences
    where
        habit_period = 'week'
        and habit_type in ('tickbox', 'number')

),

-- Monthly habits: one row per month
monthly_by_month as (

    select
        habit,
        cast(habit_date as date) as period_start,
        'month' as report_period,
        cast(1 as bigint) as total_periods,
        cast(case when is_complete then 1 else 0 end as bigint) as completed_periods
    from habit_occurrences
    where
        habit_period = 'month'
        and habit_type in ('tickbox', 'number')

),

-- HubSpot count habits: count engagements per week, compare to threshold
community_counts as (

    select
        habit,
        cast(habit_date as date) as period_start,
        cast(count(*) as bigint) as engagement_count
    from habit_occurrences
    where habit_type = 'count'
    group by habit, habit_date

),

community_by_week as (

    select
        c.habit,
        c.period_start,
        'week' as report_period,
        cast(1 as bigint) as total_periods,
        cast(
            case
                when c.engagement_count >= coalesce(hr.threshold, 1) then 1
                else 0
            end as bigint
        ) as completed_periods
    from community_counts as c
    left join habit_ref as hr on c.habit = hr.habit_key

),

all_periods as (

    select * from daily_by_week
    union all
    select * from weekly_by_week
    union all
    select * from monthly_by_month
    union all
    select * from community_by_week

)

-- Transform CTE: resolve completion rates against the Notion habit reference
,

metrics as (

    select
        {{ dbt_utils.generate_surrogate_key(['ap.habit', 'ap.period_start', 'ap.report_period']) }} as metric_key,
        ap.habit,
        hr.habit_name,
        hr.category,
        hr.frequency,
        hr.source,
        hr.habit_type,
        ap.period_start as period_start_date,
        ap.report_period,
        ap.total_periods,
        ap.completed_periods,
        cast(hr.target_pct as numeric) as target_pct,
        cast(hr.threshold as numeric) as threshold,
        hr.is_below_threshold,
        hr.is_active,
        cast(
            round(
                cast(ap.completed_periods as double) / nullif(ap.total_periods, 0),
                4
            ) as numeric
        ) as completion_rate,
        round(
            cast(ap.completed_periods as double) / nullif(ap.total_periods, 0),
            4
        ) >= hr.target_pct as target_met
    from all_periods as ap
    left join habit_ref as hr on ap.habit = hr.habit_key

)

select * from metrics
