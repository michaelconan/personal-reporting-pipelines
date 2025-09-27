-- staging model for hubspot contact data
-- TODO: use dbt_utils macro to remove duplicates by id sorting by latest date rather than custom logic
with contacts as (

    select
        id,
        properties__associatedcompanyid as company_id,
        properties__email as email,
        properties__firstname as first_name,
        properties__lastname as last_name,
        properties__createdate as created_at,
        properties__lastmodifieddate as updated_at,
        row_number() over (
            partition by id
            order by properties__lastmodifieddate desc
        ) as row_num
    from
        {% if target.name == 'dev' %}
            {{ ref('hubspot__contacts') }}
        {% else %}
            {{ source('hubspot', 'contacts') }}
        {% endif %}

)

select
    id,
    company_id,
    email,
    first_name,
    last_name,
    created_at,
    updated_at
from contacts
where row_num = 1
