-- junction table between engagements and companies
select
    {{ dbt_utils.generate_surrogate_key(['id', 'company_id']) }} as row_id,
    id as engagement_id,
    company_id
from
    {{ ref('base_hubspot__engagements') }},
    unnest(cast(json_extract(company_ids, '$') as varchar[])) as t(company_id)
where
    company_ids is not null
