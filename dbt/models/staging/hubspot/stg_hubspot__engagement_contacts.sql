-- junction table between engagements and contacts
select
    {{ dbt_utils.generate_surrogate_key(['engagement_id', 'contact_id']) }} as row_id,
    engagement_id,
    contact_id
from
    {{ ref('base_hubspot__engagements') }},
    {% if target.type == 'duckdb' %}
        unnest(cast(json_extract(contact_ids, '$') as varchar[])) as t(contact_id)
    {% elif target.type == 'bigquery' %}
        unnest(json_value_array(contact_ids)) as contact_id
    {% endif %}
where
    contact_ids is not null
