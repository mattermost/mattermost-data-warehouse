with source as (

    select * from {{ source('salesforce', 'opportunitycontactrole') }}

),

renamed as (

    select
        id as opportunity_contact_role_id,

        -- Foreign keys
        contactid as contact_id,
        createdbyid as created_by_id,
        opportunityid as opportunity_id,

        role,

        -- Custom fields
        customer_portal_access__c,

        -- Metadata
        isdeleted as is_deleted,
        isprimary as is_primary,
        createddate as created_at,
        lastmodifiedbyid as last_modified_by_id,
        lastmodifieddate as last_modified_at,
        systemmodstamp as system_modstamp_at

        -- Stitch columns omitted


    from source

)

select * from renamed
