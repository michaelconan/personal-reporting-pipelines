
    
    

with all_values as (

    select
        properties__hs_ideal_customer_profile as value_field,
        count(*) as n_records

    from "dbt"."raw"."hubspot__companies"
    group by properties__hs_ideal_customer_profile

)

select *
from all_values
where value_field not in (
    'tier_1','tier_2','tier_3'
)


