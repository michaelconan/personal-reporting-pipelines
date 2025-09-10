with sleep as (

    select
        log_id,
        date_of_sleep,
        duration,
        start_time,
        end_time,
        "type",
        log_type,
        -- Check if duration exceeded set goal
        duration >= {{ var('sleep_goal') }} as sleep_goal_met,
        row_number() over (
            partition by log_id
            order by date_of_sleep desc
        ) as row_num
    from
        {% if target.name == 'dev' %}
            {{ ref('fitbit__sleep') }}
        {% else %}
            {{ source('fitbit', 'sleep') }}
        {% endif %}

)

select
    log_id,
    date_of_sleep,
    duration,
    start_time,
    end_time,
    "type",
    log_type,
    sleep_goal_met
from
    sleep
where
    row_num = 1
