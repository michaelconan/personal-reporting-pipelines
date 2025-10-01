
    
    

with all_values as (

    select
        engagement__type as value_field,
        count(*) as n_records

    from "dbt"."raw"."hubspot__engagements"
    group by engagement__type

)

select *
from all_values
where value_field not in (
    'CALL','WHATS_APP','MEETING','SMS','EMAIL','LINKEDIN_MESSAGE','NOTE','TASK'
)


