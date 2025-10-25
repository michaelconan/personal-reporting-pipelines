-- staging model for hubspot engagement data
with engagements as (

    select
        engagement_id,
        engagement_type,
        body_preview,
        occurred_at,
        created_at,
        updated_at,
        engagement_type in ('MEETING', 'CALL') as is_synchronous
    from
        {{ ref('base_hubspot__engagements') }}

)

select *
from engagements
