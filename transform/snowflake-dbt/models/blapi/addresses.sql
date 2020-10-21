{{config({
    "materialized": "table",
    "schema": "blapi",
    "unique_key":"id",
    "tags":["hourly","blapi"]
  })
}}

WITH addresses AS (
    SELECT *
    FROM {{ source('blapi', 'addresses') }}
)

SELECT *
FROM addresses