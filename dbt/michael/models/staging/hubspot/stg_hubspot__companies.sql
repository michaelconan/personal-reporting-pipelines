-- staging model for hubspot company data

with companies as (

    select
        id,
        name,
        hs_ideal_customer_profile as tier,
        createdAt as created_at,
        updatedAt as updated_at
    from {{ source('hubspot', 'company') }}

)

select * from companies
