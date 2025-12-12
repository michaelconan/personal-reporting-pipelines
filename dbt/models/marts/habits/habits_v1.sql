with life_habits as (

    select
        {{ trunc_date('month', 'page_date') }} as habit_month,
        habit,
        avg(cast(is_complete as int)) as complete_pct
    from
        {{ ref('int_habits_unpivoted') }}
    group by
        habit_month,
        habit

),

health_habits as (

    select
        {{ trunc_date('month', 'date_of_sleep') }} as habit_month,
        'met_sleep_goal' as habit,
        avg(cast(sleep_goal_met as int)) as complete_pct
    from
        {{ ref('stg_fitbit__sleep') }}
    group by
        habit_month

),

activities as (

    select
        engagement_id,
        occurred_at,
        count(contact_id) as contacts
    from
        {{ ref('engagements') }}
    where
        is_synchronous
    group by
        engagement_id,
        occurred_at

),

community_habit_check as (

    select
        {{ trunc_date('week', 'cast(occurred_at as date)') }} as habit_week,
        case
            when contacts = 1
            then 'met_1to1'
            else
                'met_group'
        end as habit,
        count(*) >= {{ var('meet_goal') }} as is_complete
    from
        activities
    group by
        habit_week,
        habit

),

community_habits as (

    select
        {{ trunc_date('month', 'habit_week') }} as habit_month,
        habit,
        avg(cast(is_complete as int)) as complete_pct
    from
        community_habit_check
    group by
        habit_month,
        habit

)

select *
from life_habits
union all
select *
from health_habits
union all
select *
from community_habits
