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
        coalesce(existing_lead.LEAD_SOURCE_TEXT__C,'Referral') as lead_source,
        coalesce(existing_lead.LEAD_SOURCE_DETAIL__C,'Mattermost Cloud') as lead_source_detail
    FROM {{ ref('customers_with_freemium_subs') }}
    LEFT JOIN existing_lead ON customers_with_freemium_subs.email = existing_lead.email
    LEFT JOIN {{ ref('contact') }} ON customers_with_cloud_subs.email = contact.email
    WHERE contact.id is not null
    AND sku = 'cloud-enterprise' 
    AND status = 'trialing'
    AND customers_with_freemium_subs.hightouch_sync_eligible
)
SELECT * FROM freemium_leads_to_sync