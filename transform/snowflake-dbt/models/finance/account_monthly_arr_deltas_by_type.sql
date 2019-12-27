{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH account_monthly_arr_deltas_by_type AS (
    SELECT
        account_monthly_arr_deltas.month_start,
        account_monthly_arr_deltas.month_end,
        account_monthly_arr_deltas.account_sfid,
        account_monthly_arr_deltas.master_account_sfid,
        sum(CASE WHENaccount_new_arr THEN total_arr_norm_delta ELSE 0 END) AS total_arr_norm_new,
        sum(CASE WHENnot account_new_arr AND total_arr_norm_delta > 0 THEN total_arr_norm_delta ELSE 0 END) AS total_arr_norm_expansion,
        sum(CASE WHENtotal_arr_norm_delta < 0 AND coalesce(account_daily_arr.total_arr_norm,0) != 0then total_arr_norm_delta ELSE 0 END) AS total_arr_norm_contraction,
        sum(CASE WHENtotal_arr_norm_delta < 0 AND coalesce(account_daily_arr.total_arr_norm,0) = 0 THEN total_arr_norm_delta ELSE 0 END) AS total_arr_norm_churn,
        sum(total_arr_norm_delta) AS total_arr_norm_delta,
        sum(CASE WHENaccount_new_arr THEN total_arr_delta ELSE 0 END) AS total_arr_new,
        sum(CASE WHENnot account_new_arr AND total_arr_delta > 0 THEN total_arr_delta ELSE 0 END) AS total_arr_expansion,
        sum(CASE WHENtotal_arr_delta < 0 AND coalesce(account_daily_arr.total_arr,0) != 0then total_arr_delta ELSE 0 END) AS total_arr_contraction,
        sum(CASE WHENtotal_arr_delta < 0 AND coalesce(account_daily_arr.total_arr,0) = 0 THEN total_arr_delta ELSE 0 END) AS total_arr_churn,
        sum(total_arr_delta) AS total_arr_delta
    FROM {{ ref('account_monthly_arr_deltas') }}
        LEFT JOIN {{ ref('account_daily_arr') }}
            ON account_monthly_arr_deltas.account_sfid = account_daily_arr.account_sfid AND account_monthly_arr_deltas.month_end = account_daily_arr.day
    WHERE abs(total_arr_norm_delta) > 0
    GROUP BY 1, 2, 3, 4
)
