-- union daily, weekly, and monthly habits from base models
-- define specific fields to be used in the final model in CTEs

with daily_habits as (

    select
        database_id,
        'daily' as period,
        id,
        `date`,
        `name`,
        habit,
        is_complete,
        created_time,
        last_edited_time
    from {{ ref('base_notion__daily_habits') }}

),

weekly_habits as (

    select
        database_id,
        'weekly' as period,
        id,
        `date`,
        `name`,
        habit,
        is_complete,
        created_time,
        last_edited_time
    from {{ ref('base_notion__weekly_habits') }}

),

monthly_habits as (

    select
        database_id,
        'monthly' as period,
        id,
        `date`,
        `name`,
        habit,
        is_complete,
        created_time,
        last_edited_time
    from {{ ref('base_notion__monthly_habits') }}

)

select * from daily_habits

union all

select * from weekly_habits

union all

select * from monthly_habits
