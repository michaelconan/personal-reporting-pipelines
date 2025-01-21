{{ config(materialized='table') }}

-- pivot daily habits
with daily_habits as (

    select 
        database_id,
        id,
        Name as name,
        habit,
        is_complete,
        created_time,
        last_edited_time
    from {{ source('notion', 'daily_habit') }}
        unpivot(
            is_complete for habit in (Devotional, Journal, Prayer, `Read Bible`, Workout, Language)
        )

),

-- pivot weekly habits
weekly_habits as (
    
    select 
        database_id,
        id,
        Name as name,
        habit,
        is_complete,
        created_time,
        last_edited_time
    from {{ source('notion', 'weekly_habit') }}
        unpivot(
            is_complete for habit in (Fast, Church)
        )

)

-- combine daily and weekly habit pivots
select *
from daily_habits
union all
select *
from weekly_habits
