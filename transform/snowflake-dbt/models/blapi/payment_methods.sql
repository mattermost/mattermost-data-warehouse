{{config({
    "materialized": "table",
    "schema": "blp",
    "unique_key":"id",
    "tags":["nightly","blapi"],
    "database":"DEV"
  })
}}

WITH payment_methods AS (
    SELECT *
    FROM {{ source('blapi', 'payment_methods') }}
)

SELECT *
FROM payment_methods