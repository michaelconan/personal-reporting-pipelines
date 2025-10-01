
    
    

with child as (
    select properties__associatedcompanyid as from_field
    from "dbt"."raw"."hubspot__contacts"
    where properties__associatedcompanyid is not null
),

parent as (
    select id as to_field
    from "dbt"."raw"."hubspot__companies"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


