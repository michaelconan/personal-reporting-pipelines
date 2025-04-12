with raw_monthly_habit as (

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

all_monthly_habit as (

    -- select latest version of each page
    select
        database_id,
        id,
        `Date` as `date`,
        `Name` as `name`,
        habit,
        is_complete,
        created_time,
        last_edited_time
    from raw_monthly_habit
    unpivot (
        is_complete for habit in (budget, serve, travel, blog)
    )
    where row_num = 1

)

select * from all_monthly_habit
