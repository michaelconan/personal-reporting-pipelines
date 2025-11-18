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
        'sleep' as habit,
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

community_habits as (

    select
        {{ trunc_date('month', 'occurred_at') }} as habit_month,
        case
            when contacts = 1 then
                'meet_1to1'
            else
                'meet_group'
        end as habit,
        avg(count(*) >= {{ var('meet_goal') }}) as complete_pct
    from
        activities
    group by
        habit_month,
        habit

)

select *
from life_habits
union
select *
from health_habits
union
select *
from community_habits
