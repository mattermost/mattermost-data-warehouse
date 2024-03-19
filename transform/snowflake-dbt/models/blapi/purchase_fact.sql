{{config({
    "materialized": "table",
    "schema": "blapi",
    "unique_key":"id",
    "tags":["hourly","blapi", "deprecated"]
  })
}}

WITH purchase_fact AS (
    SELECT pf.*
         , s.cloud_installation_id as installation_id
    FROM {{ source('blapi', 'purchase_fact') }} pf
    LEFT JOIN {{ source('blapi', 'subscriptions') }} s
        ON pf.subscription_id = s.id
)

SELECT *
FROM purchase_fact