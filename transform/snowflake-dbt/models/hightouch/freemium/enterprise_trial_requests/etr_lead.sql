{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","freemium"]
  })
}}

WITH freemium_leads_to_sync as (
    SELECT
        customers_with_freemium_subs.email,
        customers_with_freemium_subs.domain,
        customers_with_freemium_subs.last_name
    FROM {{ ref('customers_with_freemium_subs') }}
    WHERE
    sku = 'cloud-enterprise'
    AND customers_with_freemium_subs.hightouch_sync_eligible
)
SELECT * FROM freemium_leads_to_sync