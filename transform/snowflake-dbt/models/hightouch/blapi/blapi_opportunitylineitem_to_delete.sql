{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly", "blapi", "deprecated"]
  })
}}

WITH renewal_line_items_to_delete AS (
    SELECT
        opportunitylineitem.sfid
    FROM {{ ref('customers_with_onprem_subs') }}
    JOIN {{ ref('opportunity') }}
        ON customers_with_onprem_subs.sfdc_migrated_opportunity_sfid = opportunity.sfid
    JOIN {{ ref('opportunitylineitem') }}
        ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE opportunitylineitem.subs_id__c != customers_with_onprem_subs.subscription_id
        AND customers_with_onprem_subs.hightouch_sync_eligible
)
SELECT * FROM renewal_line_items_to_delete