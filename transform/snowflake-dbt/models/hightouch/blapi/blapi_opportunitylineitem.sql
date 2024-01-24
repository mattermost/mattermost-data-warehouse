{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly", "blapi", "deprecated"]
  })
}}

WITH onprem_olis_to_sync AS (
    SELECT
        customers_with_onprem_subs.*,
        'Recurring' AS product_type,
        'New' AS product_line_type,
        'Discount' AS pricing_method,
        0 AS zero_amount,
        customers_with_onprem_subs.total / customers_with_onprem_subs.num_seats AS list_price
    FROM {{ ref('customers_with_onprem_subs') }}
    LEFT JOIN {{ ref('opportunity') }}
        ON customers_with_onprem_subs.opportunity_external_id = opportunity.dwh_external_id__c
            OR customers_with_onprem_subs.stripe_charge_id = opportunity.stripe_id__c
    LEFT JOIN {{ ref('opportunitylineitem') }}
        ON customers_with_onprem_subs.subscription_version_id = opportunitylineitem.subs_version_id__c
    LEFT JOIN {{ ref('account') }}
        ON opportunity.accountid = account.id
    WHERE opportunity.id IS NULL AND opportunitylineitem.id IS NULL
    AND customers_with_onprem_subs.sfdc_migrated_opportunity_sfid IS NULL
    AND customers_with_onprem_subs.hightouch_sync_eligible
    -- Same filters as in blapi_onprem_opportunity
    AND NOT customers_with_onprem_subs.is_renewed -- filtering out renewed from new subscriptions which are part of another model.
    AND (customers_with_onprem_subs.account_sfid is null OR customers_with_onprem_subs.account_type not in ('Customer','Customer (Attrited)')) -- removing renewals which are part of another model

)
SELECT * FROM onprem_olis_to_sync