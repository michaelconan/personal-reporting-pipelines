-- staging model for hubspot engagement data
with engagements as (

    select
        id,
        "type",
        body_preview,
        "type" in ('MEETING', 'CALL') as is_synchronous,
        "timestamp",
        created_at,
        last_updated as updated_at
    from
        {{ ref('base_hubspot__engagements') }}

)

select *
from engagements
