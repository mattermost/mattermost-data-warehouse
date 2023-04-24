
with source as (

    select * from {{ source('salesforce', 'contact') }}

),

renamed as (

    select
        id as contact_id,

        -- Foreign keys
        accountid as account_id,
        createdbyid as created_by_id,
        lastmodifiedbyid as last_modified_by_id,
        ownerid as owner_id,

        -- Contact details
        name,
        firstname as first_name,
        lastname as last_name,
        email,
        donotcall as do_not_call,
        hasoptedoutofemail as has_opted_out_of_email,
        hasoptedoutoffax as has_opted_out_of_fax,

        -- Metadata
        createddate as created_at,
        isdeleted as is_deleted,
        lastmodifieddate as last_modified_at,
        systemmodstamp as system_modstamp_at,

        -- Custom columns
        clearbit_employee_count__c,
        company_type__c,
        contact_us_inquiry_type__c,
        discoverorg_employee_count__c,
        duplicate_lead_id__c,
        dwh_external_id__c,
        employees__c,
        employee_count_override__c,
        first_action_detail__c,
        first_action__c,
        first_channel_detail__c,
        first_channel__c,
        first_mcl_date__c,
        first_mel_date__c,
        first_mql_date__c,
        first_not_a_lead_date__c,
        first_pql_date__c,
        first_qsc_date__c,
        first_qso_date__c,
        first_recycle_date__c,
        first_scl_date__c,
        first_sdl_date__c,
        how_did_you_hear_about_mattermost__c,
        lead_source_detail__c,
        lead_source__c,
        marketing_suspend__c,
        most_recent_action_detail__c,
        most_recent_action__c,
        most_recent_lead_source_detail__c,
        most_recent_lead_source__c,
        most_recent_mcl_date__c,
        most_recent_mel_date__c,
        most_recent_mql_date__c,
        most_recent_qsc_date__c,
        most_recent_qso_date__c,
        most_recent_recycle_date__c,
        most_recent_scl_date__c,
        most_recent_sdl_date__c,
        number_of_end_users__c,
        request_a_quote_date__c,
        request_to_contact_us_date__c,
        tell_us_more_about_how_we_can_help_you__c,
        trial_req_date__c,
        cloud_dns__c,
        stripe_customer_id__c,
        most_recent_pql_date__c,
        trial_license_id__c

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed
where
    not is_deleted
