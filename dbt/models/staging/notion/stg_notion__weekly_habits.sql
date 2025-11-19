-- staging model for notion weekly habit data
with weekly_habits as (

    select
        parent__database_id as database_id,
        id as page_id,
        cast(left({{ json_extract_value('properties__date__date', "'$.start'") }}, 10) as date) as page_date,
        properties__name__title as page_name,
        properties__fast__checkbox as did_fast,
        cast({{ json_extract_value('properties__prayer_minutes__formula', "'$.number'") }} as integer) as prayer_minutes,
        properties__screen_minutes__number as screen_minutes,
        properties__church__checkbox as did_church,
        properties__community__checkbox as did_community,
        created_time as created_at,
        last_edited_time as updated_at
    from
        {{ make_source('notion', 'database_weekly_habits', target.name) }}

),

unique_weekly_habits as (

    {{ dbt_utils.deduplicate(
        relation='weekly_habits',
        partition_by='page_id',
        order_by='updated_at desc',
      )
    }}

)

select * from unique_weekly_habits
