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
        lead.email
    FROM {{ ref('freemium_leads') }}
    WHERE converteddate IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY lead.email ORDER BY lead.createddate DESC) = 1
), freemium_contacts_to_sync as (
    SELECT
        customers_with_freemium_subs.contact_external_id,
        customers_with_freemium_subs.contact_sfid,
        customers_with_freemium_subs.email,
        customers_with_freemium_subs.first_name,
        COALESCE(customers_with_freemium_subs.last_name, customers_with_freemium_subs.email) AS contact_last_name,
        existing_lead.id AS duplicate_lead_id,
        -- existing_lead.email AS duplicate_lead_email,
        customers_with_freemium_subs.account_external_id,
        customers_with_freemium_subs.account_sfid,
        '0053p0000064nt8AAA' AS ownerid
    FROM {{ ref('customers_with_freemium_subs') }}
    LEFT JOIN {{ ref('contact') }} ON customers_with_freemium_subs.email = contact.email
    LEFT JOIN existing_lead ON customers_with_freemium_subs.email = existing_lead.email
    WHERE contact.id IS NULL
    AND customers_with_freemium_subs.hightouch_sync_eligible
)
SELECT * FROM freemium_contacts_to_sync