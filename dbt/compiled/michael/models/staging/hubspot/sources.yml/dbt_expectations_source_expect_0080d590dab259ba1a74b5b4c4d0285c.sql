






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and engagement__timestamp >= timestamp '2023-01-01' and engagement__timestamp <= timestamp '2030-01-01'
)
 as expression


    from "dbt"."raw"."hubspot__engagements"
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







