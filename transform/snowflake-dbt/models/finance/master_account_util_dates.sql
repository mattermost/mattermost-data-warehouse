{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH master_account_dates AS (
    SELECT 
        master_account_sfid,
        day
    FROM {{ ref('account_util_dates') }}
    GROUP BY 1,2
)
SELECT * FROM master_account_dates