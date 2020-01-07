{{config({
    "materialized": "table",
    "schema": "sales"
  })
}}

WITH quota_by_month_by_rep AS (
    SELECT 1
    FROM {{ source('sales', 'quotas_rep') }}
)
SELECT * FROM quota_by_month_by_rep
