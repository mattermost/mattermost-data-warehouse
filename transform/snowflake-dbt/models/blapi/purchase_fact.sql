{{config({
    "materialized": "table",
    "schema": "blp",
    "unique_key":"id",
    "tags":["nightly","blapi"],
    "database":"DEV"
  })
}}

WITH purchase_fact AS (
    SELECT pf.*
         , s.metadata:"cws-installation"::varchar as installation_id
    FROM {{ source('blapi', 'purchase_fact') }} pf
    LEFT JOIN {{ source('blapi', 'subscriptions') }} s
        ON pf.subscription_id = s.id
)

SELECT *
FROM purchase_fact