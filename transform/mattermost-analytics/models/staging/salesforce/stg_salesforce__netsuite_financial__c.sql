with source as (

    select * from {{ source('salesforce', 'netsuite_conn__netsuite_financial__c') }}

),

renamed as (

    select

        id as netsuite_financial__c_id,

        -- Foreign keys
        ownerid as owner_id,

        -- Standard columns
        name,

        -- Custom columns
        netsuite_conn__account__c,
        netsuite_conn__currency__c,
        netsuite_conn__days_overdue__c,
        netsuite_conn__discount_total__c,
        netsuite_conn__document_id__c,
        netsuite_conn__due_date__c,
        netsuite_conn__memo__c,
        netsuite_conn__netsuite_id__c,
        netsuite_conn__opportunity__c,
        netsuite_conn__pdf_file__c,
        netsuite_conn__status__c,
        netsuite_conn__subtotal__c,
        netsuite_conn__total__c,
        netsuite_conn__transaction_date__c,
        netsuite_conn__type__c,
        opportunity_stage__c,
        payment_method__c,

        -- Metadata
        isdeleted as is_deleted,
        createdbyid as created_by_id,
        createddate as created_at,
        lastmodifiedbyid as last_modified_by_id,
        lastmodifieddate as last_modified_at,
        systemmodstamp as system_modstamp_at

        -- Stitch columns omitted

    from source

)

select * from renamed

