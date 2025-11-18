select
    e.engagement_id,
    e.engagement_type,
    e.body_preview,
    e.occurred_at,
    e.is_synchronous,
    c.contact_id,
    c.first_name,
    c.last_name,
    a.contact_id,
    a.company_name
from
    {{ ref('stg_hubspot__engagements') }} e
left join
    {{ ref('stg_hubspot__engagement_contacts') }} ec
    on e.engagement_id = ec.engagement_id
left join
    {{ ref('stg_hubspot__contacts') }} c
    on ec.contact_id = c.contact_id
left join
    {{ ref('stg_hubspot__companies') }} a
    on c.company_id = a.company_id