






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and prayer_minutes >= 0 and prayer_minutes <= 30
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







