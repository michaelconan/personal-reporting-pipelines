






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and screen_minutes >= 500 and screen_minutes <= 1500
)
 as expression


    from "dbt"."reporting"."stg_notion__weekly_habits"
    

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







