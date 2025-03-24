with raw_daily_habit as (
    
    select
        database_id,
        id,
        `Date` as `date`,
        `Name` as `name`,
        Devotional,
        Journal,
        Prayer,
        `Read Bible`,
        Workout,
        Language,
        created_time,
        last_edited_time,
        row_number() over (
            partition by id
            order by last_edited_time desc
        ) as row_num
    from {{ source('notion', 'daily_habit') }}

),

all_daily_habit as (

    -- select latest version of each record
    select
        database_id,
        id,
        `Date` as `date`,
        `Name` as `name`,
        habit,
        is_complete,
        created_time,
        last_edited_time
    from raw_daily_habit
        unpivot(
            is_complete for habit in (Devotional, Journal, Prayer, `Read Bible`, Workout, Language)
        )
    where row_num = 1

)

select * from all_daily_habit