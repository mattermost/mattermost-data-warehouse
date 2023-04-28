{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH existing_contacts AS (
    SELECT
        contact.sfid,
        contact.email,
        contact.dwh_external_id__c,
        contact.ownerid,
        ROW_NUMBER() over (
            PARTITION BY contact.email
            ORDER BY
                createddate DESC
        ) AS row_num
    FROM
        {{ ref('contact') }}
), customers_with_cloud_paid_subs as (
    SELECT
        customers_with_cloud_paid_subs.*,
        'Customer' as account_type
    FROM {{ ref('customers_with_cloud_paid_subs') }}
    LEFT JOIN {{ ref('account') }}
        ON customers_with_cloud_paid_subs.domain = account.cbit__clearbitdomain__c
            OR customers_with_cloud_paid_subs.account_external_id = account.dwh_external_id__c
    LEFT JOIN existing_contacts
        ON customers_with_cloud_paid_subs.email = existing_contacts.email
        AND existing_contacts.row_num = 1
    WHERE account.id IS NOT NULL
        AND existing_contacts.sfid IS NULL -- create account only when contact does not exist
    AND customers_with_cloud_paid_subs.hightouch_sync_eligible
    AND customers_with_cloud_paid_subs.status in ('canceled', 'active') 
    AND customers_with_cloud_paid_subs.SKU = 'Cloud Professional'
)
SELECT * FROM customers_with_cloud_paid_subs