{{
    config({
        "materialized": "table"
    })
}}

SELECT
    user_id,
    portal_customer_id,
    min(timestamp) as first_seen_at,
    max(timestamp) as last_seen_at
FROM
    {{ ref('stg_portal_prod__identifies') }}
WHERE
    portal_customer_id IS NOT NULL 
    -- getting values as `N/A` from portal
    AND portal_customer_id NOT IN ('N/A')
    AND user_id IS NOT NULL
GROUP BY
    1,2