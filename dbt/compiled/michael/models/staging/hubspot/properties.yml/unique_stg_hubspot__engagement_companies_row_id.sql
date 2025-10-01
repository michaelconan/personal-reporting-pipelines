
    
    

select
    row_id as unique_field,
    count(*) as n_records

from "dbt"."reporting"."stg_hubspot__engagement_companies"
where row_id is not null
group by row_id
having count(*) > 1


