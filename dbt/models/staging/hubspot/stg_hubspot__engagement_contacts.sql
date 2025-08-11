-- junction table between engagements and contacts
with engagement_contacts as (

    select
        id as engagement_id,
        contact_id
    from
        {{ ref('base_hubspot__engagements') }}
    inner join
        unnest(contact_ids) as contact_id
    where contact_ids is not null

)

select *
from engagement_contacts
