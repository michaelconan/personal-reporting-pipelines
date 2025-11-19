with daily_habits_unpivoted as (

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as row_id,
        'day' as habit_period,
        database_id,
        page_id,
        page_date,
        page_name,
        habit,
        is_complete,
        created_at,
        updated_at
    from {{ ref('stg_notion__daily_habits') }}
    unpivot (
        is_complete for habit in (
            did_devotional, did_journal, did_prayer, did_read_bible, did_workout, did_language
        )
    )

),

weekly_habits_unpivoted as (

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as row_id,
        'week' as habit_period,
        database_id,
        page_id,
        page_date,
        page_name,
        habit,
        is_complete,
        created_at,
        updated_at
    from {{ ref('stg_notion__weekly_habits') }}
    unpivot (
        is_complete for habit in (
            did_community, did_fast, did_church
        )
    )

),

monthly_habits_unpivoted as (

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as row_id,
        'month' as habit_period,
        database_id,
        page_id,
        page_date,
        page_name,
        habit,
        is_complete,
        created_at,
        updated_at
    from {{ ref('stg_notion__monthly_habits') }}
    unpivot (
        is_complete for habit in (
            did_serve, did_travel, did_budget, did_blog
        )
    )

)

select *
from daily_habits_unpivoted
union all
select *
from weekly_habits_unpivoted
union all
select *
from monthly_habits_unpivoted
