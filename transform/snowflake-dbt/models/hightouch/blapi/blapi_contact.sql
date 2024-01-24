{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi", "deprecated"]
  })
}}

WITH existing_lead AS (
    SELECT
        lead.id,
        lead.email,
        ROW_NUMBER() OVER (PARTITION BY lead.email ORDER BY lead.createddate DESC) as row_num
    FROM {{ ref('lead') }}
    WHERE converteddate IS NULL
), onprem_contacts_to_sync as (
    SELECT
        customers_with_onprem_subs.*,
        COALESCE(customers_with_onprem_subs.last_name, customers_with_onprem_subs.email) AS contact_last_name,
        existing_lead.id AS duplicate_lead_id,
        '0053p0000064nt8AAA' AS ownerid
    FROM {{ ref('customers_with_onprem_subs') }}
    LEFT JOIN {{ ref('contact') }} ON customers_with_onprem_subs.email = contact.email
    LEFT JOIN existing_lead ON customers_with_onprem_subs.email = existing_lead.email
    WHERE contact.id IS NULL
    AND customers_with_onprem_subs.hightouch_sync_eligible
)
SELECT * FROM onprem_contacts_to_sync