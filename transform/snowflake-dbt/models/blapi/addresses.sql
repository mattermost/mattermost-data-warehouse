{{config({
    "materialized": "incremental",
    "schema": "blp",
    "unique_key":"id",
    "tags":"nightly"
  })
}}

WITH addresses AS (
    SELECT *
    FROM {{ source('blapi', 'addresses') }}
)