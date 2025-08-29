-- base model to remove duplicate engagements before creating junction tables

with engagements as (

    select
        engagement__id as id,
        engagement__type as "type",
        engagement__timestamp as "timestamp",
        engagement__body_preview as body_preview,
        string_to_array(replace(replace(associations__company_ids, '[', ''), ']', ''), ',') as company_ids,
        string_to_array(replace(replace(associations__contact_ids, '[', ''), ']', ''), ',') as contact_ids,
        engagement__created_at as created_at,
        engagement__last_updated as last_updated,
        row_number() over (
            partition by engagement__id
            order by engagement__last_updated desc
        ) as row_num
    from
        {{ source('hubspot', 'engagements') }}

)

select *
from
    engagements
where
    row_num = 1
