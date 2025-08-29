with daily_habit_unpivoted as (

    select
        database_id,
        id,
        "date",
        "name",
        habit,
        is_complete,
        created_time,
        last_edited_time
    from {{ ref('stg_notion__daily_habits') }}
    unpivot (
        is_complete for habit in (
            devotional, journal, prayer, read_bible, workout, language
        )
    )

),

weekly_habit_unpivoted as (

    select
        database_id,
        id,
        "date",
        "name",
        habit,
        is_complete,
        created_time,
        last_edited_time
    from {{ ref('stg_notion__weekly_habits') }}
    unpivot (
        is_complete for habit in (
            community, fast, church
        )
    )

),

monthly_habit_unpivoted as (

    select
        database_id,
        id,
        "date",
        "name",
        habit,
        is_complete,
        created_time,
        last_edited_time
    from {{ ref('stg_notion__monthly_habits') }}
    unpivot (
        is_complete for habit in (
            serve, travel, budget, blog
        )
    )

)

select *
from daily_habit_unpivoted
union all
select *
from weekly_habit_unpivoted
union all
select *
from monthly_habit_unpivoted
