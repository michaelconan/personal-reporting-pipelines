-- junction table between engagements and companies
select
    {{ dbt_utils.generate_surrogate_key(['engagement_id', 'company_id']) }} as row_id,
    engagement_id,
    company_id
from
    {{ ref('base_hubspot__engagements') }},
    {% if target.type == 'duckdb' %}
        unnest(cast(json_extract(company_ids, '$') as varchar[])) as t(company_id)
    {% elif target.type == 'bigquery' %}
        unnest(json_value_array(company_ids)) as company_id
    {% endif %}
where
    company_ids is not null
