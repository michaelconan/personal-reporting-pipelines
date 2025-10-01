-- junction table between engagements and companies
select
    md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(company_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as row_id,
    id as engagement_id,
    company_id
from
    "dbt"."reporting"."base_hubspot__engagements",
    unnest(cast(json_extract(company_ids, '$') as varchar[])) as t(company_id)
where
    company_ids is not null