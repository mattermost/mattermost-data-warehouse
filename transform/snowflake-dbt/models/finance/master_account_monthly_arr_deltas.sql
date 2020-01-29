{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH master_account_monthly_arr_deltas AS (
    SELECT
        date_trunc('month',new_day) AS month_start,
        date_trunc('month',new_day) + interval '1 month' - interval '1 day' AS month_end,
        master_account_sfid,
        master_account_new_arr,
        sum(total_arr_delta) AS total_arr_delta
    FROM {{ ref('master_account_daily_arr_deltas') }}
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM master_account_monthly_arr_deltas