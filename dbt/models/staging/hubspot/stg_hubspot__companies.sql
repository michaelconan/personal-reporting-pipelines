-- staging model for hubspot company data
with companies as (

    select
        id,
        `name`,
        createdat as created_at,
        updatedat as updated_at,
        -- parse numeric value from tier label
        parse_numeric(right(hs_ideal_customer_profile, 1)) as tier,
        row_number() over (
            partition by id
            order by updatedat desc
        ) as row_num
    from {{ source('hubspot', 'company') }}

)

select
    id,
    `name`,
    tier,
    created_at,
    updated_at
from companies
where row_num = 1
