
    
    

select
    log_id as unique_field,
    count(*) as n_records

from "dbt"."reporting"."stg_fitbit__sleep"
where log_id is not null
group by log_id
having count(*) > 1


