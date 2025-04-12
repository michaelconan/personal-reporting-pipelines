with weekly_habit as (

    select
        database_id,
        id,
        `Date` as `date`,
        `Name` as `name`,
        `Fast` as `fast`,
        `Prayer Minutes` as prayer_minutes,
        `Screen Minutes` as screen_minutes,
        church,
        community,
        created_time,
        last_edited_time,
        row_number() over (
            partition by id
            order by last_edited_time desc
        ) as row_num
    from {{ source('notion', 'weekly_habit') }}

),

select
    database_id,
    id,
    `date`,
    `name`,
    `fast`,
    prayer_minutes,
    screen_minutes,
    church,
    community,
    created_time,
    last_edited_time
from
    weekly_habit
where 
    row_num = 1
