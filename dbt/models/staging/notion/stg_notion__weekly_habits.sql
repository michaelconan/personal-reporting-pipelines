-- TODO: use dbt_utils macro to remove duplicates by id sorting by latest date rather than custom logic
with weekly_habit as (

    select
        parent__database_id as database_id,
        id,
        properties__date__date as date,
        properties__name__title as name,
        properties__fast__checkbox as fast,
        properties__prayer_minutes__formula as prayer_minutes,
        properties__screen_minutes__number as screen_minutes,
        properties__church__checkbox as church,
        properties__community__checkbox as community,
        created_time,
        last_edited_time,
        row_number() over (
            partition by id
            order by last_edited_time desc
        ) as row_num
        from
        {% if target.name == 'dev' %}
            {{ ref('notion__database_weekly_habits') }}
        {% else %}
            {{ source('notion', 'database_weekly_habits') }}
        {% endif %}

)
select
    database_id,
    id,
    date,
    name,
    fast,
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
