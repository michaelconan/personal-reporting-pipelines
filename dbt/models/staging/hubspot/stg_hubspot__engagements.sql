-- staging model for hubspot engagement data
with engagements as (

    select
        id,
        engagement_type,
        body_preview,
        engagement_type in ('MEETING', 'CALL') as is_synchronous,
        engagement_timestamp,
        created_at,
        last_updated as updated_at
    from
        {{ ref('base_hubspot__engagements') }}

)

select *
from engagements
