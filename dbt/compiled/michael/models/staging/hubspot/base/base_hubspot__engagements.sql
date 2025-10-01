-- base model to remove duplicate engagements before creating junction tables
-- TODO: use dbt_utils macro to remove duplicates by id sorting by latest date rather than custom logic

with engagements as (

    select
        engagement__id as id,
        engagement__type as engagement_type,
        engagement__timestamp as engagement_timestamp,
        engagement__body_preview as body_preview,
        associations__company_ids as company_ids,
        associations__contact_ids as contact_ids,
        engagement__created_at as created_at,
        engagement__last_updated as last_updated,
        row_number() over (
            partition by engagement__id
            order by engagement__last_updated desc
        ) as row_num
    from
        
            "dbt"."reporting"."hubspot__engagements"
        

)

select *
from
    engagements
where
    row_num = 1