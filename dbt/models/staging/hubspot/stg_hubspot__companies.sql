-- staging model for hubspot company data
with companies as (

    select
        id as company_id,
        properties__name as company_name,
        properties__createdate as created_at,
        properties__hs_lastmodifieddate as updated_at,
        -- parse numeric value from tier label
        {{ cast_safe("right(properties__hs_ideal_customer_profile, 1)", "integer") }} as company_tier
    from
        {{ make_source('hubspot', 'companies', target.name) }}

),

unique_companies as (

    {{ dbt_utils.deduplicate(
        relation='companies',
        partition_by='company_id',
        order_by='updated_at desc',
      )
    }}

)

select * from unique_companies
