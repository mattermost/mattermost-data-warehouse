with source as (

    select * from {{ source('salesforce', 'lead') }}

),

renamed as (

    select
        id as lead_id,

        -- Foreign keys
        convertedaccountid as converted_account_id,
        convertedcontactid as converted_contact_id,
        convertedopportunityid as converted_opportunity_id,
        createdbyid as created_by_id,
        lastmodifiedbyid as last_modified_by_id,
        ownerid as owner_id,

        -- Conversion status
        converteddate as converted_at,
        status,

        -- Details
        name,
        description,
        firstname as first_name,
        lastname as last_name,

        -- Contact preferences
        hasoptedoutofemail as has_opted_out_of_email,
        hasoptedoutoffax as has_opted_out_of_fax,

        -- Contact details
        country,
        countrycode as country_code,
        state,
        statecode,
        postalcode,
        phone,
        mobilephone as mobile_phone,
        email,

        -- Company context
        company,
        industry,
        website,
        numberofemployees,

        -- Metadata
        createddate as created_at,
        isdeleted as is_deleted,
        lastmodifieddate as last_modified_at,
        systemmodstamp as system_modstamp_at,

        -- Custom columns
        actively_being_sequenced__c,
        api_id__c,
        assignment_logic__c,
        behavior_score__c,
        campaign_id__c,
        channel_detail__c,
        channel__c,
        cleaned_up_website__c,
        clearbit_employee_count__c,
        cloud_daily_posts__c,
        cloud_dau__c,
        cloud_dns__c,
        cloud_last_active_date__c,
        cloud_mau__c,
        cloud_plugins_enabled__c,
        cloud_total_posts__c,
        company_type__c,
        connected__c,
        contact_us_inquiry_type__c,
        current_sequence_status__c,
        current_sequence_step_number__c,
        current_sequence_step_type__c,
        current_sequence_task_due_date__c,
        current_sequence_user_name__c,
        discoverorg_employee_count__c,
        discovery_call_booked__c,
        discovery_call_completed__c,
        dscorgpkg__company_hq_country_code__c,
        dwh_external_id__c,
        employee_count_override__c,
        engagio__matched_account__c,
        existing_account__c,
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
        geo__c,
        how_did_you_hear_about_mattermost__c,
        inbound_outbound__c,
        indirect_lead__c,
        industry_text__c,
        job_function__c,
        junk_reason__c,
        lead2convdays__c,
        lead2trialdays__c,
        lead_created_date__c,
        lead_number__c,
        lead_source_detail__c,
        lead_source_text__c,
        lead_status_at_conversion__c,
        lead_status_minor__c,
        lead_type__c,
        linkedin__c,
        marketing_suspend__c,
        matched_account_company_type__c,
        matched_account_named_account_tier__c,
        matched_account_named_account__c,
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
        name_of_currently_active_sequence__c,
        never_connected__c,
        original_owner__c,
        outreach_cadence_add__c,
        outreach_manual_create__c,
        outreach__c,
        partner_email__c,
        partner_name__c,
        product_status__c,
        quality_star_rating__c,
        quality__c,
        recycle_reason__c,
        request_a_demo_date__c,
        request_a_trial_date__c,
        request_quote_date__c,
        request_to_contact_us_date__c,
        stripe_customer_id__c,
        tell_us_more_about_how_we_can_help_you__c,
        territoryid__c,
        territory__c,
        timeline__c,
        where_are_you_with_mattermost__c,
        discovery_call_date__c,
        most_recent_pql_date__c,
        lifecycle_stage__c,
        sal_date__c,
        most_recent_reengaged_date__c,
        first_reengaged_date__c,
        trial_license_id__c,
        trial_activation_date__c

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed
where
    not is_deleted