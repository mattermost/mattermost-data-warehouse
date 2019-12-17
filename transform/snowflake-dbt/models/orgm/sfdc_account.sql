{{config({
    "materialized": "table",
    "schema": "orgm"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'account') }}

)

select *
from source