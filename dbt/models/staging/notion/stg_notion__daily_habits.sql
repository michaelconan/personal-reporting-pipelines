with daily_habit as (

    select
        database_id,
        id,
        `Date` as `date`,
        `Name` as `name`,
        devotional,
        journal,
        prayer,
        `Read Bible` as read_bible,
        workout,
        `Language` as `language`,
        created_time,
        last_edited_time,
        row_number() over (
            partition by id
            order by last_edited_time desc
        ) as row_num
    from {{ source('notion', 'daily_habit') }}

),

select
    database_id,
    id,
    `date`,
    `name`,
    devotional,
    journal,
    prayer,
    read_bible,
    workout,
    `language`,
    created_time,
    last_edited_time
from
    daily_habit
where 
    row_num = 1
