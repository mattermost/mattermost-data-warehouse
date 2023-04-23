
with source as (

    select * from {{ source('salesforce', 'campaignmember') }}

),

renamed as (

    select
        id as campaign_member_id,
        campaignid as campaign_id,
        city,
        companyoraccount as company_or_account,
        contactid as contact_id,
        country,
        createdbyid as created_by_id,
        createddate as created_at,
        description,
        donotcall as do_not_call,
        email,
        fax,
        firstname as first_name,
        firstrespondeddate as first_responded_at,
        hasoptedoutofemail as has_opted_out_of_email,
        hasoptedoutoffax as has_opted_out_of_fax,
        hasresponded as has_responded,
        isdeleted as is_deleted,
        lastmodifiedbyid as last_modified_by_id,
        lastmodifieddate as last_modified_at,
        lastname as last_name,
        leadid as lead_id,
        leadorcontactid as lead_or_contact_id,
        leadorcontactownerid as lead_or_contact_owner_id,
        leadsource as lead_source,
        mobilephone as mobile_phone,
        name,

        phone,
        postalcode,
        salutation,
        state,
        status,
        street,
        systemmodstamp as system_modstamp_at,
        title,
        type,

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