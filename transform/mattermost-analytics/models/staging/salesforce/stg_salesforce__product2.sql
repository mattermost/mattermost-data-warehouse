with source as (

    select * from {{ source('salesforce', 'product2') }}

),

renamed as (

    select
        id as product2_id,

        -- Foreign keys
        createdbyid as created_by_id,
        lastmodifiedbyid as last_modified_by_id,

        -- Details
        name,
        description,
        family,
        isactive as is_active,
        productcode as product_code,

        -- Metadata
        createddate as created_at,
        isdeleted as is_deleted,
        lastmodifieddate as last_modified_at,
        systemmodstamp as system_modstamp_at,
        lastvieweddate as last_viewed_at,
        lastreferenceddate as last_reference_date,

        -- Custom columns

        netsuite_conn__celigo_update__c,
        netsuite_conn__netsuite_id__c,
        netsuite_conn__push_to_netsuite__c,
        netsuite_conn__sync_in_progress__c,
        pricing_type__c,
        product_id_18_digit__c,
        product_id__c,
        required_term_length_days__c

        -- Stitch columns omitted

    from source
)

select * from renamed
