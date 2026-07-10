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
        below_threshold,
        active
    from {{ ref('stg_notion__habit_reference') }}

),

-- Join occurrences with reference and resolve is_complete per occurrence.
-- tickbox: 1.0 = done
-- number (above threshold): habit_value >= threshold
-- number (below threshold): habit_value <= threshold
-- count (HubSpot met_*): aggregated separately below; is_complete left null here
habit_occurrences as (

    select
        h.habit,
        h.habit_date,
        h.habit_period,
        h.habit_value,
        hr.habit_type,
        hr.threshold,
        hr.below_threshold,
        hr.target_pct,
        hr.habit_name,
        hr.category,
        hr.frequency,
        hr.source,
        hr.active,
        case
            when hr.habit_type = 'tickbox'
                then h.habit_value = 1.0
            when hr.habit_type = 'number' and not hr.below_threshold
                then h.habit_value >= hr.threshold
            when hr.habit_type = 'number' and hr.below_threshold
                then h.habit_value <= hr.threshold
        end as is_complete
    from habits h
    left join habit_ref hr on h.habit = hr.habit_key

),

-- Daily tickbox + number habits: aggregate each calendar week
daily_by_week as (

    select
        habit,
        {{ trunc_date('week', 'habit_date') }} as period_start,
        'week' as report_period,
        count(*) as total_periods,
        sum(case when is_complete then 1 else 0 end) as completed_periods
    from habit_occurrences
    where habit_period = 'day'
      and habit_type in ('tickbox', 'number')
    group by habit, {{ trunc_date('week', 'habit_date') }}

),

-- Weekly tickbox + number habits: one row per week, already at correct grain
weekly_by_week as (

    select
        habit,
        habit_date as period_start,
        'week' as report_period,
        1 as total_periods,
        case when is_complete then 1 else 0 end as completed_periods
    from habit_occurrences
    where habit_period = 'week'
      and habit_type in ('tickbox', 'number')

),

-- Monthly habits: one row per month
monthly_by_month as (

    select
        habit,
        habit_date as period_start,
        'month' as report_period,
        1 as total_periods,
        case when is_complete then 1 else 0 end as completed_periods
    from habit_occurrences
    where habit_period = 'month'
      and habit_type in ('tickbox', 'number')

),

-- HubSpot count habits: count engagements per week, compare to threshold
community_counts as (

    select
        habit,
        habit_date as period_start,
        count(*) as engagement_count
    from habit_occurrences
    where habit_type = 'count'
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

all_periods as (

    select * from daily_by_week
    union all
    select * from weekly_by_week
    union all
    select * from monthly_by_month
    union all
    select * from community_by_week

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
