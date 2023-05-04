{{
    config({
        "materialized": "table"
    })
}}

SELECT
    user_id,
    portal_customer_id,
    max(timestamp) as first_seen_at,
    max(timestamp) as last_seen_at
FROM
    {{ ref('stg_portal_prod__identifies') }}
WHERE
    portal_customer_id IS NOT NULL
    AND user_id IS NOT NULL
GROUP BY
    1,2
