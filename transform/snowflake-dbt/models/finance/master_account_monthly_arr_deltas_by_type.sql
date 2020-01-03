{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH master_account_monthly_arr_deltas_by_type AS (
    SELECT
        master_account_monthly_arr_deltas.month_start,
        master_account_monthly_arr_deltas.month_end,
        master_account_monthly_arr_deltas.master_account_sfid,
        sum(CASE WHEN account_new_arr THEN total_arr_norm_delta ELSE 0 END) AS total_arr_norm_new,
        sum(CASE WHEN not account_new_arr AND total_arr_norm_delta > 0 THEN total_arr_norm_delta ELSE 0 END) AS total_arr_norm_expansion,
        sum(CASE WHEN total_arr_norm_delta < 0 AND coalesce(master_account_daily_arr.total_arr_norm,0) != 0then total_arr_norm_delta ELSE 0 END) AS total_arr_norm_contraction,
        sum(CASE WHEN total_arr_norm_delta < 0 AND coalesce(master_account_daily_arr.total_arr_norm,0) = 0 THEN total_arr_norm_delta ELSE 0 END) AS total_arr_norm_churn,
        sum(total_arr_norm_delta) AS total_arr_norm_delta,
        sum(CASE WHEN account_new_arr THEN total_arr_delta ELSE 0 END) AS total_arr_new,
        sum(CASE WHEN not account_new_arr AND total_arr_delta > 0 THEN total_arr_delta ELSE 0 END) AS total_arr_expansion,
        sum(CASE WHEN total_arr_delta < 0 AND coalesce(master_account_daily_arr.total_arr,0) != 0then total_arr_delta ELSE 0 END) AS total_arr_contraction,
        sum(CASE WHEN total_arr_delta < 0 AND coalesce(master_account_daily_arr.total_arr,0) = 0 THEN total_arr_delta ELSE 0 END) AS total_arr_churn,
        sum(total_arr_delta) AS total_arr_delta
    FROM {{ ref('master_account_monthly_arr_deltas') }}
        LEFT JOIN {{ ref('master_account_daily_arr') }}
            ON master_account_monthly_arr_deltas.master_account_sfid = master_account_daily_arr.master_account_sfid AND master_account_monthly_arr_deltas.month_end = master_account_daily_arr.day
    WHERE abs(total_arr_norm_delta) > 0
    GROUP BY 1, 2, 3
)
select * from master_account_monthly_arr_deltas_by_type