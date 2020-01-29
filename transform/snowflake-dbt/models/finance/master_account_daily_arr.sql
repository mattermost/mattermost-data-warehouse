{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH master_account_daily_arr AS (
  SELECT
    master_account_sfid,
  	day,
    SUM(total_arr) AS total_arr
  FROM {{ ref('account_daily_arr') }}
  GROUP BY 1, 2
)
SELECT * FROM master_account_daily_arr