{{config({
    "materialized": "table",
    "schema": "blp",
    "unique_key":"id",
    "tags":"nightly"
  })
}}

WITH credit_cards AS (
    SELECT *
    FROM {{ source('blapi', 'credit_cards') }}
)

SELECT *
FROM credit_cards