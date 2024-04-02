{{config({
    "materialized": "table",
    "schema": "blapi",
    "unique_key":"id",
    "tags":["hourly","blapi", "deprecated"]
  })
}}

WITH payment_methods AS (
    SELECT *
    FROM {{ source('blapi', 'payment_methods') }}
)

SELECT *
FROM payment_methods