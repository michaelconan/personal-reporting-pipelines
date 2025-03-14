-- staging model for hubspot engagement data

with engagements as (

    select *
    from {{ source('hubspot', 'engagement') }}

)

select * from engagements
