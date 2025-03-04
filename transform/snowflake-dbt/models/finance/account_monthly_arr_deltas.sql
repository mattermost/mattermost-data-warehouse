{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":["nightly"]
  })
}}

WITH account_monthly_arr_deltas AS (
    SELECT
        date_trunc('month',new_day) AS month_start,
        date_trunc('month',new_day) + interval '1 month' - interval '1 day' AS month_end,
        master_account_sfid,
        account_sfid,
        max(account_new_arr) as account_new_arr,
        max(coalesce(CASE WHEN month_start THEN previous_day_total_arr ELSE 0 END, 0)) as month_starting_arr,
        max(coalesce(CASE WHEN month_end THEN new_day_total_arr ELSE 0 END, 0)) as month_ending_arr,
        sum(total_arr_delta) AS total_arr_delta
    FROM {{ ref('account_daily_arr_deltas') }}
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM account_monthly_arr_deltas