-- staging model for hubspot company data

with companies as (

    select *
    from {{ source('hubspot', 'company') }}

)

select * from companies
