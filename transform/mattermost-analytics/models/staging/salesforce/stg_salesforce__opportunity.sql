
with source as (

    select * from {{ source('salesforce', 'opportunity') }}

),

renamed as (

    select
        id as opportunity_id,

        -- Foreign keys
        accountid as account_id,
        campaignid as campaign_id,
        contractid as contract_id,
        createdbyid as created_by_id,
        lastmodifiedbyid as last_modified_by_id,
        ownerid as owner_id,
        pricebook2id as pricebook2_id,

        -- Context
        name,
        type,

        -- Pipeline
        probability,
        expectedrevenue as expected_revenue,
        forecastcategory as forecast_category,
        forecastcategoryname as forecast_category_name,
        closedate as close_at,
        isclosed as is_closed,
        iswon as is_won,
        nextstep as next_step,
        stagename as stage_name,

        -- Accounting
        amount,

        -- Metadata
        createddate as created_at,
        isdeleted as is_deleted,
        lastmodifieddate as last_modified_at,
        systemmodstamp as system_modstamp_at,

        -- Custom columns
        active_licenses__c,
        additional_environment_details__c,
        amount_in_commit__c,
        amount_in_pipeline__c,
        available_renewal__c,
        billing_city__c,
        billing_country__c,
        billing_country_code__c,
        billing_partner_name__c,
        billing_state_province__c,
        billing_street__c,
        billing_zip_postal_code__c,
        ce_owner__c,
        closed_lost_other__c,
        compelling_event__c,
        competitor__c,
        contract_finalized__c,
        created_by_role__c,
        cs_qualified_lead__c,
        csm_override__c,
        csm_owner__c,
        current_identitiy_provider_sso__c,
        current_identity_provider_sso__c,
        current_productivity_platform__c,
        data_migration_required__c,
        days_past_renewal__c,
        decision_criteria_process__c,
        delta_amount__c,
        dwh_external_id__c,
        e_purchase_date__c,
        emm_mdm__c,
        end_customer_po_number__c,
        ending_arr__c,
        enterprise_trial_completed__c,
        existing_chat_solution__c,
        existing_chat_solution_details__c,
        expansion_type__c,
        extended_support_release_customer__c,
        forecast_category_custom__c,
        geo__c,
        gtm_save_motions__c,
        how_did_you_hear_about_mattermost__c,
        infosec_questionnaire_completed__c,
        invoice_number__c,
        lead_created_date__c,
        lead_source_detail__c,
        lead_source_detail_upon_conversion__c,
        lead_source_text__c,
        lead_source_upon_conversion__c,
        lead_type__c,
        leadid__c,
        license_active__c,
        license_contract_sent__c,
        license_end_date__c,
        license_key__c,
        license_key_sent_date__c,
        license_quantity__c,
        license_start_date__c,
        lost_reason__c,
        lost_to_competitor__c,
        mobile_in_scope__c,
        mql_date__c,
        mql_reason__c,
        new_expansion_total__c,
        new_logo__c,
        open_source_user__c,
        opp_line_items_products__c,
        opp_products__c,
        order_type__c,
        original_opportunity__c,
        original_opportunity_amount__c,
        original_opportunity_arr__c,
        original_opportunity_end_date__c,
        original_opportunity_id__c,
        original_opportunity_length_in_months__c,
        original_opportunityid__c,
        other_integrations__c,
        override_total_net_new_arr__c,
        paper_trail_process__c,
        partner_name__c,
        product__c,
        regulatory_requirements__c,
        renewal_amount_total__c,
        renewal_created_date__c,
        renewal_includes_leftover_expansion__c,
        renewed_by_opp_arr__c,
        renewed_by_opp_prob__c,
        renewed_by_opportunity_id__c,
        requirements__c,
        risks__c,
        sbqq__contracted__c,
        sbqq__primaryquote__c,
        self_service_enabled__c,
        shipping_city__c,
        shipping_country__c,
        shipping_country_code__c,
        shipping_state_province__c,
        shipping_street__c,
        shipping_zip_postal_code__c,
        stage_before_closed_lost__c,
        stage_change_date__c,
        status_wlo__c,
        stripe_id__c,
        subs_id__c,
        suppress_external_email__c,
        suppress_internal_email__c,
        suppress_renewal_process__c,
        target_go_live_date__c,
        target_integrations__c,
        territory__c,
        territory_segment__c,
        time_in_stage__c,
        total_net_new_arr__c,
        total_net_new_arr_with_override__c,
        trial_license_id__c,
        use_case__c,

        -- Derived columns
        replace(
            coalesce(
                regexp_substr(name, '(\\d+,\\d+)\\s+Total', 1, 1, 'ei', 1),
                regexp_substr(name, '(\\d+)\\s+Total', 1, 1, 'ei', 1),
                regexp_substr(name, '(\\d+,\\d+)\\s+Seats Total', 1, 1, 'ei', 1),
                regexp_substr(name, '(\\d+)\\s+Seats Total', 1, 1, 'ei', 1),
                regexp_substr(name, 'Total\\s+(\\d+,\\d+)', 1, 1, 'ei', 1),
                regexp_substr(name, 'Total\\s+(\\d+)', 1, 1, 'ei', 1),
                regexp_substr(name, '(\\d+,\\d+)\\s+Seat', 1, 1, 'ei', 1),
                regexp_substr(name, '(\\d+)\\s+Seat', 1, 1, 'ei', 1),
                regexp_substr(name, '(\\d+) - Seat', 1, 1, 'ei', 1),
                regexp_substr(name, '(\\d+)_seat', 1, 1, 'ei', 1),
                regexp_substr(name, 'qty:(\\d+)', 1, 1, 'ei', 1),
                regexp_substr(name, '(\\d+)\\s+Users', 1, 1, 'ei', 1),
                regexp_substr(name, '(\\d+)-Users', 1, 1, 'ei', 1),
                regexp_substr(name, 'E10/(\\d+)', 1, 1, 'ei', 1),
                regexp_substr(name, 'E10-(\\d+)', 1, 1, 'ei', 1)
            ), ','
        )::int as seats_from_name

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed
where
    not is_deleted
