-- junction table between engagements and companies
select
    id as engagement_id,
    company_id
from
    {{ ref('base_hubspot__engagements') }},
    unnest(company_ids) as t(company_id)
where
    company_ids is not null
