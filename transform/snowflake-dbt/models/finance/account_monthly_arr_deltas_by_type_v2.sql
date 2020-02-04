{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH account_monthly_arr_deltas_by_type_v2 AS (
    SELECT
        account_monthly_arr_deltas.month_start,
        account_monthly_arr_deltas.month_end,
        account_monthly_arr_deltas.account_sfid,
        account_monthly_arr_deltas.master_account_sfid,
        CASE 
            WHEN account_new_arr THEN 'New ARR'
            WHEN not account_new_arr AND total_arr_delta > 0 THEN 'Expansion ARR'
            WHEN total_arr_delta < 0 AND coalesce(account_daily_arr.total_arr,0) != 0 THEN 'Contraction ARR'
            WHEN total_arr_delta < 0 AND coalesce(account_daily_arr.total_arr,0) = 0 THEN 'Churn ARR'
            ELSE NULL
        END as arr_type,
        sum(CASE WHEN account_new_arr THEN total_arr_delta ELSE 0 END) AS total_arr_new,
        sum(CASE WHEN not account_new_arr AND total_arr_delta > 0 THEN total_arr_delta ELSE 0 END) AS total_arr_expansion,
        sum(CASE WHEN total_arr_delta < 0 AND coalesce(account_daily_arr.total_arr,0) != 0 THEN total_arr_delta ELSE 0 END) AS total_arr_contraction,
        sum(CASE WHEN total_arr_delta < 0 AND coalesce(account_daily_arr.total_arr,0) = 0 THEN total_arr_delta ELSE 0 END) AS total_arr_churn,
        sum(total_arr_delta) AS total_arr_delta
    FROM {{ ref('account_monthly_arr_deltas') }}
        LEFT JOIN {{ ref('account_daily_arr') }}
            ON account_monthly_arr_deltas.account_sfid = account_daily_arr.account_sfid AND account_monthly_arr_deltas.month_end = account_daily_arr.day
    WHERE abs(total_arr_delta) > 0
    GROUP BY 1, 2, 3, 4, 5
)
select * from account_monthly_arr_deltas_by_type_v2