{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}
with existing_contacts as (
    select
        contact.sfid,
        contact.email,
        contact.dwh_external_id__c,
        contact.ownerid,
        row_number() over (partition by contact.email order by createddate desc) as row_num
    from {{ ref('contact') }}
), existing_leads as (
    select
        lead.sfid,
        lead.email,
        lead.dwh_external_id__c,
        lead.ownerid,
        row_number() over (partition by lead.email order by createddate desc) as row_num
    from {{ ref('lead') }}
), customers_with_cloud_paid_subs as (
    SELECT
        customers_with_cloud_paid_subs.*,
        '0053p0000064nt8AAA' AS ownerid
    FROM {{ ref('customers_with_cloud_paid_subs') }}
    LEFT JOIN {{ ref('account') }}
        ON customers_with_cloud_paid_subs.domain = account.cbit__clearbitdomain__c
            OR customers_with_cloud_paid_subs.account_external_id = account.dwh_external_id__c
    LEFT JOIN existing_contacts ON customers_with_cloud_paid_subs.email = existing_contacts.email and existing_contacts.row_num = 1
    LEFT JOIN existing_leads ON customers_with_cloud_paid_subs.email = existing_leads.email and existing_leads.row_num = 1
    WHERE account.id IS NULL
    AND customers_with_cloud_paid_subs.hightouch_sync_eligible
    AND customers_with_cloud_paid_subs.status in ('canceled', 'active') 
    AND customers_with_cloud_paid_subs.SKU = 'Cloud Professional'
)
SELECT * FROM customers_with_cloud_paid_subs