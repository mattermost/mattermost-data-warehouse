{{config({
    "materialized": "incremental",
    "schema": "blp",
    "unique_key":"id",
    "tags":"nightly"
  })
}}

WITH payment_methods AS (
    SELECT *
    FROM {{ source('blapi', 'payment_methods') }}
    
)