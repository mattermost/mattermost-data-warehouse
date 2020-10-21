{{config({
    "materialized": "table",
    "schema": "blp",
    "unique_key":"id",
    "tags":["nightly","blapi"],
    "database":"DEV"
  })
}}

WITH credit_cards AS (
    SELECT *
    FROM {{ source('blapi', 'credit_cards') }}
)

SELECT *
FROM credit_cards