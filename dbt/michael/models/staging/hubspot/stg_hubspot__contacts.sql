-- staging model for hubspot contact data
with contacts as (

    select
        id,
        associatedcompanyid as company_id,
        email,
        firstname as first_name,
        lastname as last_name,
        createdat as created_at,
        updatedat as updated_at,
        row_number() over (
            partition by id
            order by updatedat desc
        ) as row_num
    from
        {{ source('hubspot', 'contact') }}

)

select
    id,
    company_id,
    first_name,
    last_name,
    created_at,
    updated_at
from contacts
where row_num = 1
