{{config({
    "materialized": "table",
    "schema": "blapi",
    "unique_key":"id",
    "tags":["hourly","blapi", "deprecated"]
  })
}}

WITH credit_cards AS (
    SELECT *
    FROM {{ source('blapi', 'credit_cards') }}
)

SELECT *
FROM credit_cards