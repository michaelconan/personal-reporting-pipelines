-- staging model for hubspot company data
with companies as (

    select
        id as company_id,
        properties__name as company_name,
        properties__createdate as created_at,
        properties__hs_lastmodifieddate as updated_at,
        -- parse numeric value from tier label
        {% if target.type == 'bigquery' %}
            -- bigquery function
            safe_cast(
        {% else %}     
            -- duckdb and most other adapaters
            try_cast(
        {% endif %}
            right(properties__hs_ideal_customer_profile, 1) as integer) as company_tier
        from
            {% if target.name == 'dev' %}
                {{ ref('hubspot__companies') }}
            {% else %}
                {{ source('hubspot', 'companies') }}
            {% endif %}

),

unique_companies as (

  {{ deduplicate(
      relation='companies',
      partition_by='company_id',
      order_by='updated_at desc',
     )
  }}

)

select * from unique_companies
