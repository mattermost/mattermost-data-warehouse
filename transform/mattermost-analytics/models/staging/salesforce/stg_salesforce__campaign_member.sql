
with source as (

    select * from {{ source('salesforce', 'campaignmember') }}

),

renamed as (

    select
        id as campaign_member_id,

        -- Foreign keys
        campaignid as campaign_id,
        createdbyid as created_by_id,
        leadid as lead_id,
        lastmodifiedbyid as last_modified_by_id,
        leadorcontactid as lead_or_contact_id,
        leadorcontactownerid as lead_or_contact_owner_id,
        contactid as contact_id,

        -- Details
        name,
        salutation,
        description,
        status,
        title,
        type,
        companyoraccount as company_or_account,
        firstname as first_name,
        lastname as last_name,
        leadsource as lead_source,

        -- Contact preferences
        donotcall as do_not_call,
        firstrespondeddate as first_responded_at,
        hasoptedoutofemail as has_opted_out_of_email,
        hasoptedoutoffax as has_opted_out_of_fax,
        hasresponded as has_responded,

        -- Contact information
        phone,
        mobilephone as mobile_phone,
        fax,
        email,

        -- Address
        street,
        city,
        country,
        state,
        postalcode,

        -- Metadata
        createddate as created_at,
        isdeleted as is_deleted,
        lastmodifieddate as last_modified_at,
        systemmodstamp as system_modstamp_at,

        -- Custom columns
        accounttype__c,
        contact_us_notes__c,
        converted_date__c,
        cosize__c,
        dwh_external_id__c,
        e_purchase_date__c,
        first_responded_on__c,
        geo__c,
        last_occurrence__c,
        leadcontact_create_date__c,
        lead_source_text__c,
        lead_source__c,
        marketing_ops_fix_detail__c,
        member_channel_detail__c,
        member_channel__c,
        mql_date__c,
        mql__c,
        named_account__c,
        pql_date__c,
        region__c,
        senior_title__c,
        technical_title__c

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed
where
    not is_deleted