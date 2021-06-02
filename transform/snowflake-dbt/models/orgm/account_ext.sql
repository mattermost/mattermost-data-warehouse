{{config({
    "materialized": "table",
    "schema": "orgm"
  })
}}


WITH account_customer_journey_fannout AS (
    SELECT
        account.sfid as account_sfid,
	    FIRST_VALUE(opportunity.use_case__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS use_case,
	    FIRST_VALUE(opportunity.compelling_event__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS compelling_event,
	    FIRST_VALUE(opportunity.competitor__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS competitor,
	    FIRST_VALUE(opportunity.target_integrations__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS target_integrations,
	    FIRST_VALUE(opportunity.other_integrations__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS other_integrations,
	    FIRST_VALUE(opportunity.current_identity_provider_sso__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS current_identitiy_provider_sso,
	    FIRST_VALUE(opportunity.emm_mdm__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS emm_mdm,
	    FIRST_VALUE(opportunity.extended_support_release_customer__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS extended_support_release_customer,
	    FIRST_VALUE(opportunity.current_productivity_platform__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS current_productivity_platform,
	    FIRST_VALUE(opportunity.regulatory_requirements__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS regulatory_requirements,
	    FIRST_VALUE(opportunity.additional_environment_details__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS additional_environment_details,
	    FIRST_VALUE(opportunity.how_did_you_hear_about_mattermost__c IGNORE NULLS) OVER (PARTITION BY account.sfid ORDER BY closedate desc) AS how_did_you_hear_about_mattermost
    FROM {{ ref('account') }}
    LEFT JOIN {{ ref('opportunity') }} ON account.sfid = opportunity.accountid
), account_customer_journey AS (
    SELECT *
    FROM account_customer_journey_fannout
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
), account_ext AS (
    SELECT
        account.sfid as account_sfid,
        use_case,
        compelling_event,
        competitor,
        target_integrations,
        other_integrations,
        current_identitiy_provider_sso,
        emm_mdm,
        extended_support_release_customer,
        current_productivity_platform,
        regulatory_requirements,
        additional_environment_details,
        how_did_you_hear_about_mattermost,
        MIN(CASE WHEN opportunity.iswon AND (opportunity.new_logo__c OR opportunity.type = 'New Subscription') THEN opportunity.closedate ELSE NULL END) as min_close_won_date,
        COUNT(DISTINCT CASE WHEN opportunity.iswon THEN opportunity.sfid ELSE NULL END) AS count_won_oppt,
        COUNT(DISTINCT CASE WHEN NOT opportunity.isclosed THEN opportunity.sfid ELSE NULL END) AS count_open_oppt,
        COUNT(DISTINCT CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN opportunity.sfid ELSE NULL END) AS count_lost_oppt,
        SUM(CASE WHEN opportunity.iswon THEN opportunity_ext.sum_new_amount ELSE 0 END) AS sum_new_amount_won,
        SUM(CASE WHEN opportunity.iswon THEN opportunity_ext.sum_expansion_amount ELSE 0 END) AS sum_expansion_amount_won,
        SUM(CASE WHEN opportunity.iswon THEN opportunity_ext.sum_renewal_amount ELSE 0 END) AS sum_renewal_amount_won,
        SUM(CASE WHEN opportunity.iswon THEN opportunity_ext.sum_multi_amount ELSE 0 END) AS sum_multi_amount_won,
        SUM(CASE WHEN NOT opportunity.isclosed THEN opportunity_ext.sum_new_amount ELSE 0 END) AS sum_new_amount_open,
        SUM(CASE WHEN NOT opportunity.isclosed THEN opportunity_ext.sum_expansion_amount ELSE 0 END) AS sum_expansion_amount_open,
        SUM(CASE WHEN NOT opportunity.isclosed THEN opportunity_ext.sum_renewal_amount ELSE 0 END) AS sum_renewal_amount_open,
        SUM(CASE WHEN NOT opportunity.isclosed THEN opportunity_ext.sum_multi_amount ELSE 0 END) AS sum_multi_amount_open,
        SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN opportunity_ext.sum_new_amount ELSE 0 END) AS sum_new_amount_lost,
        SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN opportunity_ext.sum_expansion_amount ELSE 0 END) AS sum_expansion_amount_lost,
        SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN opportunity_ext.sum_renewal_amount ELSE 0 END) AS sum_renewal_amount_lost,
        SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN opportunity_ext.sum_multi_amount ELSE 0 END) AS sum_multi_amount_lost
    FROM {{ ref('account') }}
    LEFT JOIN {{ ref('opportunity') }} ON account.sfid = opportunity.accountid
    LEFT JOIN {{ ref('opportunity_ext') }} ON account.sfid = opportunity_ext.accountid
    LEFT JOIN account_customer_journey ON account.sfid = account_customer_journey.account_sfid
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
)

SELECT * FROM account_ext