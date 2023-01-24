{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}
-- create contact
WITH existing_lead AS (
    SELECT
        lead.id,
        lead.email,
        lead.ownerid
    FROM {{ ref('lead') }}
    WHERE converteddate IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY lead.email ORDER BY lead.createddate DESC) = 1
), freemium_contacts_to_sync as (
    SELECT
        customers_with_cloud_paid_subs.email,
        customers_with_cloud_paid_subs.first_name,
        COALESCE(customers_with_cloud_paid_subs.last_name, customers_with_cloud_paid_subs.email) AS contact_last_name,
        existing_lead.id AS duplicate_lead_id,
        -- existing_lead.email AS duplicate_lead_email,
        customers_with_cloud_paid_subs.account_external_id,
        COALESCE(existing_lead.ownerid, '0053p0000064nt8AAA') AS ownerid
        'Cloud Purchase' AS most_recent_action,
        'Cloud Professional' AS most_recent_action_detail,
        'Referral' AS lead_source,
        'Cloud Starter' AS lead_source_detail
    FROM {{ ref('customers_with_cloud_paid_subs') }}
    LEFT JOIN {{ ref('contact') }} ON customers_with_cloud_paid_subs.email = contact.email
    LEFT JOIN existing_lead ON customers_with_cloud_paid_subs.email = existing_lead.email
    WHERE contact.id IS NULL
    AND customers_with_cloud_paid_subs.hightouch_sync_eligible
    AND customers_with_cloud_paid_subs.status in ('canceled', 'active') 
    AND customers_with_cloud_paid_subs.SKU = 'Cloud Professional'
)
SELECT * FROM freemium_contacts_to_sync