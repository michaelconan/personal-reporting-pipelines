-- staging model for hubspot company data
-- TODO: use dbt_utils macro to remove duplicates by id sorting by latest date rather than custom logic
with companies as (

    select
        id,
        properties__name as "name",
        properties__createdate as created_at,
        properties__hs_lastmodifieddate as updated_at,
        -- parse numeric value from tier label
        try_cast(right(properties__hs_ideal_customer_profile, 1) as int) as tier,
        row_number() over (
            partition by id
            order by properties__hs_lastmodifieddate desc
        ) as row_num
        from
        
            "dbt"."reporting"."hubspot__companies"
        

)

select
    id,
    "name",
    tier,
    created_at,
    updated_at
from companies
where row_num = 1