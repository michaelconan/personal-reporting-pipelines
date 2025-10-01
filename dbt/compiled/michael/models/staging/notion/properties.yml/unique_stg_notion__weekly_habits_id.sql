
    
    

select
    id as unique_field,
    count(*) as n_records

from "dbt"."reporting"."stg_notion__weekly_habits"
where id is not null
group by id
having count(*) > 1


