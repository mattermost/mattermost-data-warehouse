{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":["nightly"]
  })
}}

WITH account_daily_arr AS (
  SELECT
    account_sfid,
    master_account_sfid,
  	day,
  	SUM(won_arr)::int AS total_arr
  FROM {{ ref( 'opportunity_daily_arr') }}
  WHERE won_arr <> 0
  GROUP BY 1, 2, 3
)
SELECT * FROM account_daily_arr