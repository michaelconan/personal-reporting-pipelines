-- staging model for notion daily habit data
with daily_habits as (

    select
        parent__database_id as database_id,
        id as page_id,
        cast(left({{ json_extract_value('properties__date__date', "'$.start'") }}, 10) as date)
          as page_date,
        properties__name__title as page_name,
        properties__devotional__checkbox as did_devotional,
        properties__journal__checkbox as did_journal,
        properties__prayer__checkbox as did_prayer,
        properties__read_bible__checkbox as did_read_bible,
        properties__workout__checkbox as did_workout,
        properties__language__checkbox as did_language,
        created_time as created_at,
        last_edited_time as updated_at
        from
            {% if target.name == 'dev' %}
                {{ ref('notion__database_daily_habits') }}
            {% else %}
                {{ source('notion', 'database_daily_habits') }}
            {% endif %}

),

unique_daily_habits as (

  {{ deduplicate(
      relation='daily_habits',
      partition_by='page_id',
      order_by='updated_at desc',
     )
  }}

)

select * from unique_daily_habits
