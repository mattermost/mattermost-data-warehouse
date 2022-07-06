{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH customers_with_cloud_paid_subs as (
    SELECT
        customers_with_cloud_paid_subs.*,
        'Customer (Attrited)' as account_type
    FROM {{ ref('customers_with_cloud_paid_subs') }}
    LEFT JOIN {{ ref('account') }}
        ON customers_with_cloud_paid_subs.domain = account.cbit__clearbitdomain__c
            OR customers_with_cloud_paid_subs.account_external_id = account.dwh_external_id__c
    LEFT JOIN {{ source('orgm', 'account_domain_mapping') }}
        ON customers_with_cloud_paid_subs.domain = account_domain_mapping.domain
    WHERE account.id IS NOT NULL AND account_domain_mapping.accountid IS NOT NULL
    AND customers_with_cloud_paid_subs.hightouch_sync_eligible 
    AND customers_with_cloud_paid_subs.status = 'canceled' 
    AND customers_with_cloud_paid_subs.SKU = 'Cloud Professional'
)
SELECT * FROM customers_with_cloud_paid_subs