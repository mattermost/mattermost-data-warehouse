{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH freemium_opportunity_to_sync AS (
    SELECT
        customers_with_freemium_subs.*,
        customers_with_freemium_subs.start_date as closed_date
    FROM {{ ref('customers_with_freemium_subs') }}
    LEFT JOIN {{ ref('opportunity') }}
        ON customers_with_freemium_subs.opportunity_external_id = opportunity.dwh_external_id__c
    LEFT JOIN {{ ref('account') }}
        ON customers_with_freemium_subs.account_external_id = account.dwh_external_id__c
    WHERE opportunity.id IS NOT NULL
    AND customers_with_freemium_subs.hightouch_sync_eligible
    AND customers_with_freemium_subs.status = 'canceled' 
    AND customers_with_freemium_subs.SKU = 'Cloud Professional'
)
SELECT * FROM freemium_opportunity_to_sync