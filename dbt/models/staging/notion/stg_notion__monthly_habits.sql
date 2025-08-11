with monthly_habit as (

    select
        database_id,
        id,
        `Date` as `date`,
        `Name` as `name`,
        budget,
        serve,
        travel,
        blog,
        created_time,
        last_edited_time,
        row_number() over (
            partition by id
            order by last_edited_time desc
        ) as row_num
    from {{ source('notion', 'monthly_habit') }}

),

select
    database_id,
    id,
    `date`,
    `name`,
    budget,
    serve,
    travel,
    blog,
    created_time,
    last_edited_time
from
    monthly_habit
where 
    row_num = 1
