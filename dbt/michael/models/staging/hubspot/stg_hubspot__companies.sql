-- staging model for hubspot company data

with companies as (

    select
        id,
        name,
        -- parse numeric value from tier label
        parse_numeric(right(hs_ideal_customer_profile, 1)) as tier,
        createdAt as created_at,
        updatedAt as updated_at
    from {{ source('hubspot', 'company') }}

)

select * from companies
