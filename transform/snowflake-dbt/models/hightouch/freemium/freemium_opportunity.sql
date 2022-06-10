{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH freemium_opportunity_to_sync AS (
    SELECT
        customers_with_freemium_subs.*,
        SPLIT_PART(cloud_dns, '.', 0 ||
            ' New Subscription') AS opportunity_name,
        'New Subscription' AS opportunity_type,
        0 as new_new_arr_override,
        '0053p0000064nt8AAA' AS ownerid,
        -- '0056u0000010szhAAA' AS ownerid,
        '6. Closed Won' as stage
    FROM {{ ref('customers_with_freemium_subs') }}
    LEFT JOIN {{ ref('opportunity') }}
        ON customers_with_freemium_subs.opportunity_external_id = opportunity.dwh_external_id__c
    LEFT JOIN {{ ref('account') }}
        ON customers_with_freemium_subs.account_external_id = account.dwh_external_id__c
    WHERE opportunity.sfid IS NULL
        AND customers_with_freemium_subs.hightouch_sync_eligible
)
SELECT * FROM freemium_opportunity_to_sync