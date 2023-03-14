{{ config(
    { "schema": "hightouch",
    "materialized": "view",
    "tags" :["hourly","blapi"] }
) }}

WITH existing_contacts AS (

    SELECT
        contact.sfid,
        contact.email,
        contact.dwh_external_id__c,
        CASE
            WHEN SUBSTR(
                contact.ownerid,
                0,
                3
            ) = '00G' THEN NULL
            ELSE contact.ownerid
        END AS ownerid,
        ROW_NUMBER() over (
            PARTITION BY contact.email
            ORDER BY
                createddate DESC
        ) AS row_num
    FROM
        {{ ref('contact') }}
),
existing_leads AS (
    SELECT
        LEAD.sfid,
        LEAD.email,
        LEAD.dwh_external_id__c,
        CASE
            WHEN SUBSTR(
                LEAD.ownerid,
                0,
                3
            ) = '00G' THEN NULL
            ELSE LEAD.ownerid
        END AS ownerid,
        ROW_NUMBER() over (
            PARTITION BY LEAD.email
            ORDER BY
                createddate DESC
        ) AS row_num
    FROM
        {{ ref('lead') }}
),
customers_with_cloud_paid_subs AS (
    SELECT
        customers_with_cloud_paid_subs.*,
        COALESCE(
            existing_leads.ownerid,
            existing_contacts.ownerid,
            '0053p0000064nt8AAA'
        ) AS ownerid
    FROM
        {{ ref('customers_with_cloud_paid_subs') }}
        LEFT JOIN {{ ref('account') }}
        ON customers_with_cloud_paid_subs.domain = account.cbit__clearbitdomain__c
        OR customers_with_cloud_paid_subs.account_external_id = account.dwh_external_id__c
        LEFT JOIN existing_contacts
        ON customers_with_cloud_paid_subs.email = existing_contacts.email
        AND existing_contacts.row_num = 1
        LEFT JOIN existing_leads
        ON customers_with_cloud_paid_subs.email = existing_leads.email
        AND existing_leads.row_num = 1
    WHERE
        account.id IS NULL
        AND existing_contacts.sfid IS NULL -- create account only when contact does not exist
        AND customers_with_cloud_paid_subs.hightouch_sync_eligible
        AND customers_with_cloud_paid_subs.status IN (
            'canceled',
            'active'
        )
        AND customers_with_cloud_paid_subs.sku = 'Cloud Professional'
)
SELECT
    *
FROM
    customers_with_cloud_paid_subs
