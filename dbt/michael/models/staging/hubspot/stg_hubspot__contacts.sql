-- staging model for hubspot contact data

with contacts as (

    select *
    from {{ source('hubspot', 'contact') }}

)

select * from contacts
