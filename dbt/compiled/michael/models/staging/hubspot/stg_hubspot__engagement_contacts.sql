-- junction table between engagements and contacts
select
    md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(contact_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as row_id,
    id as engagement_id,
    contact_id
from
    "dbt"."reporting"."base_hubspot__engagements",
    unnest(cast(json_extract(contact_ids, '$') as varchar[])) as t(contact_id)
where
    contact_ids is not null