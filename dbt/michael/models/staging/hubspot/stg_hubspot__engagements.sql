-- staging model for hubspot engagement data

with engagements as (

    select
        id,
        type,
        timestamp_millis(timestamp) as timestamp,
        bodyPreview as body_preview,
        contactIds as contact_ids,
        companyIds as company_ids,
        timestamp_millis(createdAt) as created_at,
        timestamp_millis(lastUpdated) as updated_at
    from {{ source('hubspot', 'engagement') }}

)

select * from engagements
