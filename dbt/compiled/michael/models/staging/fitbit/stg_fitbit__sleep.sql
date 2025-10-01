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
        duration >= 25200000 as sleep_goal_met,
        row_number() over (
            partition by log_id
            order by date_of_sleep desc
        ) as row_num
    from
        
            "dbt"."reporting"."fitbit__sleep"
        

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