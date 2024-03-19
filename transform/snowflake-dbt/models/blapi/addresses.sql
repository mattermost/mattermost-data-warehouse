{{config({
    "materialized": "table",
    "schema": "blapi",
    "unique_key":"id",
    "tags":["hourly","blapi", "deprecated"]
  })
}}

WITH addresses AS (
    SELECT *
    FROM {{ source('blapi', 'addresses') }}
)

SELECT *
FROM addresses