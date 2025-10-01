-- TODO: use dbt_utils macro to remove duplicates by id sorting by latest date rather than custom logic
with monthly_habit as (

    select
        parent__database_id as database_id,
        id,
        properties__date__date as date,
        properties__name__title as name,
        properties__budget__checkbox as budget,
        properties__serve__checkbox as serve,
        properties__travel__checkbox as travel,
        properties__blog__checkbox as blog,
        created_time,
        last_edited_time,
        row_number() over (
            partition by id
            order by last_edited_time desc
        ) as row_num
        from
        
            "dbt"."reporting"."notion__database_monthly_habits"
        

)
select
    database_id,
    id,
    date,
    name,
    budget,
    serve,
    travel,
    blog,
    created_time,
    last_edited_time
from
    monthly_habit
where
    row_num = 1