-- junction table between engagements and contacts
select
    {{ dbt_utils.generate_surrogate_key(['id', 'contact_id']) }} as row_id,
    id as engagement_id,
    contact_id
from
    {{ ref('base_hubspot__engagements') }},
    unnest(cast(json_extract(contact_ids, '$') as varchar[])) as t(contact_id)
where
    contact_ids is not null
