{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH master_account_dates AS (
    SELECT 
        master_account_sfid,
        min(min_start_date) AS min_start_date,
        max(max_end_date) AS max_end_date
    FROM {{ ref('account_util_dates') }}
    GROUP BY 1
)
SELECT * FROM master_account_dates