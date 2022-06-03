{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","freemium"]
  })
}}
WITH existing_lead AS (
    SELECT
        lead.id,
        lead.email,
        ROW_NUMBER() OVER (PARTITION BY lead.email ORDER BY lead.createddate DESC) as row_num
    FROM {{ ref('lead') }}
    WHERE converteddate IS NULL
),WITH freemium_leads_to_sync as (
    SELECT
        customers_with_freemium_subs.email,
        customers_with_freemium_subs.domain,
        customers_with_freemium_subs.last_name,
        'Trial Request' as most_recent_action,
        'Cloud Enterprise' as most_recent_action_detail,
        'Referral' as lead_source,
        'Cloud Started' as lead_source_detail
    FROM {{ ref('customers_with_freemium_subs') }}
    LEFT JOIN existing_lead ON customers_with_freemium_subs.email = existing_lead.email
    WHERE
    sku = 'cloud-enterprise'
    AND customers_with_freemium_subs.hightouch_sync_eligible
)
SELECT * FROM freemium_leads_to_sync