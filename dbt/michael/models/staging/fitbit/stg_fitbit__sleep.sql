with sleep as (

    select
        logid as log_id,
        dateofsleep as date_of_sleep,
        duration,
        starttime as start_time,
        endtime as end_time,
        `type`,
        logtype as log_type,
        row_number() over (
            partition by logid
            order by dateofsleep desc
        ) as row_num
    from
        {{ source('fitbit', 'sleep') }}

)

select
    log_id,
    date_of_sleep,
    duration,
    start_time,
    end_time,
    `type`,
    log_type
from
    sleep
where
    row_num = 1
