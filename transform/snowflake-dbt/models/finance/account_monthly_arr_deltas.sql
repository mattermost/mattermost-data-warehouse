{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH account_monthly_arr_deltas AS (
    SELECT
        date_trunc('month',new_day) AS month_start,
        date_trunc('month',new_day) + interval '1 month' - interval '1 day' AS month_end,
        master_account_sfid,
        account_sfid,
        account_new_arr,
        coalesce(max(starting_month_total_arr),0) as month_starting_arr,
        coalesce(max(ending_month_total_arr),0) as month_ending_arr,
        sum(total_arr_delta) AS total_arr_delta
    FROM {{ ref('account_daily_arr_deltas') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM account_monthly_arr_deltas