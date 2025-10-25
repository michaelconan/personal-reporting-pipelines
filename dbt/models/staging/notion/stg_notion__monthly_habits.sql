-- staging model for notion monthly habit data
with monthly_habits as (

    select
        parent__database_id as database_id,
        id as page_id,
        cast(left({{ json_extract_value('properties__date__date', "'$.start'") }}, 10) as date)
          as page_date,
        properties__name__title as page_name,
        properties__budget__checkbox as did_budget,
        properties__serve__checkbox as did_serve,
        properties__travel__checkbox as did_travel,
        properties__blog__checkbox as did_blog,
        created_time as created_at,
        last_edited_time as updated_at
        from
            {% if target.name == 'dev' %}
                {{ ref('notion__database_monthly_habits') }}
            {% else %}
                {{ source('notion', 'database_monthly_habits') }}
            {% endif %}

),

unique_monthly_habits as (

  {{ deduplicate(
      relation='monthly_habits',
      partition_by='page_id',
      order_by='updated_at desc',
     )
  }}

)

select * from unique_monthly_habits
