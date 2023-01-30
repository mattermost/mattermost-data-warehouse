{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH customers_with_cloud_paid_subs as (
    SELECT
        customers_with_cloud_paid_subs.*,
        'Customer' as account_type
    FROM {{ ref('customers_with_cloud_paid_subs') }}
    LEFT JOIN {{ ref('account') }}
        ON customers_with_cloud_paid_subs.domain = account.cbit__clearbitdomain__c
            OR customers_with_cloud_paid_subs.account_external_id = account.dwh_external_id__c
    WHERE account.id IS NOT NULL
    AND customers_with_cloud_paid_subs.hightouch_sync_eligible
    AND customers_with_cloud_paid_subs.status in ('canceled', 'active') 
    AND customers_with_cloud_paid_subs.SKU = 'Cloud Professional'
)
SELECT * FROM customers_with_cloud_paid_subs