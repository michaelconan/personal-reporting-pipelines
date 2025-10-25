-- staging model for hubspot contact data
-- TODO: use dbt_utils macro to remove duplicates by id sorting by latest date rather than custom logic
with contacts as (

    select
        id as contact_id,
        properties__associatedcompanyid as company_id,
        properties__email as email,
        properties__firstname as first_name,
        properties__lastname as last_name,
        properties__createdate as created_at,
        properties__lastmodifieddate as updated_at
    from
        {% if target.name == 'dev' %}
            {{ ref('hubspot__contacts') }}
        {% else %}
            {{ source('hubspot', 'contacts') }}
        {% endif %}

),

unique_contacts as (

  {{ deduplicate(
      relation='contacts',
      partition_by='contact_id',
      order_by='updated_at desc',
     )
  }}

)

select * from unique_contacts
