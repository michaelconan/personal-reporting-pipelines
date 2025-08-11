-- junction table between engagements and companies
with engagement_companies as (

    select
        id as engagement_id,
        company_id
    from
        {{ ref('base_hubspot__engagements') }}
    inner join
        unnest(company_ids) as company_id
    where company_ids is not null

)

select *
from engagement_companies
