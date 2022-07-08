{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH freemium_opportunity_to_sync AS (
    SELECT
        customers_with_cloud_paid_subs.*,
        SPLIT_PART(cloud_dns, '.', 0) ||
            ' Cloud Subscription FY' || RIGHT(SPLIT_PART(customers_with_cloud_paid_subs.end_date, '-', 0),2) AS opportunity_name,
        'New Subscription' AS opportunity_type,
        0 as new_new_arr_override,
        '0053p0000064nt8AAA' AS ownerid,
        '0056u0000010szhAAA' AS sandbox_ownerid,
        '6. Closed Won' as stage,
        'Online' as order_type
    FROM {{ ref('customers_with_cloud_paid_subs') }}
    LEFT JOIN {{ ref('opportunity') }}
        ON customers_with_cloud_paid_subs.opportunity_external_id = opportunity.dwh_external_id__c
    LEFT JOIN {{ ref('account') }}
        ON customers_with_cloud_paid_subs.account_external_id = account.dwh_external_id__c
    WHERE opportunity.id IS NULL
    AND customers_with_cloud_paid_subs.hightouch_sync_eligible
    AND customers_with_cloud_paid_subs.status in ('canceled', 'active') 
    AND customers_with_cloud_paid_subs.SKU = 'Cloud Professional'
)
SELECT * FROM freemium_opportunity_to_sync