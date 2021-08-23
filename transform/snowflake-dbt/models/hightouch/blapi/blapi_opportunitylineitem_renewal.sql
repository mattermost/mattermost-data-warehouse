{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH onprem_olis_to_sync AS (
    SELECT
        customers_with_onprem_subs.*,
        'Recurring' AS product_type,
        'Mixed' AS product_line_type,
        'Discount' AS pricing_method,
        0 AS zero_amount,
        customers_with_onprem_subs.total - customers_with_onprem_subs.renewed_from_total as expansion_amount,
        customers_with_onprem_subs.total / customers_with_onprem_subs.num_seats AS list_price
    FROM {{ ref('customers_with_onprem_subs') }}
    JOIN {{ ref('opportunity') }}
        ON customers_with_onprem_subs.sfdc_migrated_opportunity_sfid = opportunity.id
    LEFT JOIN {{ ref('opportunitylineitem') }}
        ON customers_with_onprem_subs.subscription_version_id = opportunitylineitem.subs_version_id__c
    WHERE opportunitylineitem.id IS NULL
    AND customers_with_onprem_subs.hightouch_sync_eligible
)
SELECT * FROM onprem_olis_to_sync