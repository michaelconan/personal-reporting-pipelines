-- base model to remove duplicate engagements before creating junction tables

with engagements as (

    select
        id,
        `type`,
        `timestamp`,
        bodypreview as body_preview,
        companyids as company_ids,
        contactids as contact_ids,
        createdat,
        lastupdated,
        row_number() over (
            partition by id
            order by lastupdated desc
        )
    from
        {{ source('hubspot', 'engagement') }}

)

select *
from
    engagements
where
    row_num = 1
