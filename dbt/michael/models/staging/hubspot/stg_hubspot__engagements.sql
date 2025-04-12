-- staging model for hubspot engagement data
with engagements as (

    select
        id,
        `type`,
        bodypreview as body_preview,
        `type` in ('MEETING', 'CALL') as is_synchronous,
        timestamp_millis(`timestamp`) as `timestamp`,
        timestamp_millis(createdat) as created_at,
        timestamp_millis(lastupdated) as updated_at
    from
        {{ ref('base_hubspot__engagements') }}

)

select *
from engagements
