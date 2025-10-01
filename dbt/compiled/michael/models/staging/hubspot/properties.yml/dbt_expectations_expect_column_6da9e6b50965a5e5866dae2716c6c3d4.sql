






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and tier >= 1 and tier <= 3
)
 as expression


    from "dbt"."reporting"."stg_hubspot__companies"
    where
        tier is not null
    
    

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







