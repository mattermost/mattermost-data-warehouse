{{config({
    "materialized": "table",
    "schema": "orgm"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('orgm', 'account') }}

)

select *
from source