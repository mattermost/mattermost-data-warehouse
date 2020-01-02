{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH master_account_daily_arr AS (
  SELECT
    master_account_sfid,
  	day,
    SUM(total_arr) AS total_arr,
  	SUM(total_arr_norm) AS total_arr_norm
  FROM {{ ref('master_account_daily_arr') }}
  GROUP BY 1, 2
)
SELECT * FROM master_account_daily_arr