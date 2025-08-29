-- junction table between engagements and contacts
select
    id as engagement_id,
    contact_id
from
    {{ ref('base_hubspot__engagements') }},
    unnest(cast(json_extract(contact_ids, '$') as varchar[])) as t(contact_id)
where
    contact_ids is not null
