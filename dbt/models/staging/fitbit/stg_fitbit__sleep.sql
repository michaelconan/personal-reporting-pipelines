-- staging model from fitbit sleep data
with sleep as (

    select
        log_id,
        cast(date_of_sleep as date) as date_of_sleep,
        duration as duration_ms,
        duration / 3600000 as duration_hr,
        start_time as started_at,
        end_time as ended_at,
        type as sleep_type,
        log_type,
        -- Check if duration exceeded set goal
        duration >= {{ var('sleep_goal') }} as sleep_goal_met
    from
        {{ make_source('fitbit', 'sleep') }}

),

unique_sleep as (

    {{ dbt_utils.deduplicate(
        relation='sleep',
        partition_by='log_id',
        order_by='date_of_sleep desc',
        )
    }}

)

select * from unique_sleep
