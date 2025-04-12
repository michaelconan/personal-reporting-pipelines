with raw_weekly_habit as (

    select
        database_id,
        id,
        `Date` as `date`,
        `Name` as `name`,
        fast,
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

all_weekly_habit as (

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
    from raw_weekly_habit
    unpivot (
        is_complete for habit in (fast, church, community)
    )
    where row_num = 1

)

select * from all_weekly_habit
