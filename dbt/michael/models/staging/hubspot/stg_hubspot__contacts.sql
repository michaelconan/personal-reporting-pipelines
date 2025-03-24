-- staging model for hubspot contact data

with contacts as (

    select
        id,
        associatedcompanyid as company_id,
        email,
        firstname as first_name,
        lastname as last_name,
        createdAt as created_at,
        updatedAt as updated_at
    from {{ source('hubspot', 'contact') }}

)

select * from contacts
