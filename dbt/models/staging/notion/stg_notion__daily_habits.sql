-- TODO: use dbt_utils macro to remove duplicates by id sorting by latest date rather than custom logic
with daily_habit as (

    select
        parent__database_id as database_id,
        id,
        properties__date__date as date,
        properties__name__title as name,
        properties__devotional__checkbox as devotional,
        properties__journal__checkbox as journal,
        properties__prayer__checkbox as prayer,
        properties__read_bible__checkbox as read_bible,
        properties__workout__checkbox as workout,
        properties__language__checkbox as language,
        created_time,
        last_edited_time,
        row_number() over (
            partition by id
            order by last_edited_time desc
        ) as row_num
        from
        {% if target.name == 'dev' %}
            {{ ref('notion__database_daily_habits') }}
        {% else %}
            {{ source('notion', 'database_daily_habits') }}
        {% endif %}

)
select
    database_id,
    id,
    date,
    name,
    devotional,
    journal,
    prayer,
    read_bible,
    workout,
    language,
    created_time,
    last_edited_time
from
    daily_habit
where
    row_num = 1
