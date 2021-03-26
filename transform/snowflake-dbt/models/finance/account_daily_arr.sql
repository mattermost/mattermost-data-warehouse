{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH account_daily_arr AS (
  SELECT
    account_sfid,
    master_account_sfid,
  	day,
  	SUM(total_arr)::int AS total_arr
  FROM {{ ref( 'opportunity_daily_arr') }}
  GROUP BY 1, 2, 3
)
SELECT * FROM account_daily_arr