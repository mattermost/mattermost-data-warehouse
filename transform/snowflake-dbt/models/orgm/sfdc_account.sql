{{config({
    "materialized": "table"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('orgm', 'account') }}
)

select *
from source