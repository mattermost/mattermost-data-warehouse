{{config({
    "materialized": "incremental",
    "schema": "blp",
    "unique_key":"id",
    "tags":["nightly","blapi"],
    "database":"DEV"
  })
}}

WITH addresses AS (
    SELECT *
    FROM {{ source('blapi', 'addresses') }}
)