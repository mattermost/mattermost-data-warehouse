{{config({
    "materialized": "table",
    "schema": "sales"
  })
}}

WITH quota_by_month_by_rep AS (
    SELECT 1
    FROM {{ ref('quota_by_month_by_rep') }}
), acv_by_month_by_rep AS (
    SELECT 1
    FROM {{ source('orgm','opportunity')}}
), tva_rep AS (
    Blah
)
SELECT * FROM tva_rep