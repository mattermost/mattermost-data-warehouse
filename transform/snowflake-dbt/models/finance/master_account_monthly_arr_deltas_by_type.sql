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
        CASE 
            WHEN account_new_arr THEN 'New ARR'
            WHEN not account_new_arr AND total_arr_delta > 0 THEN 'Expansion ARR'
            WHEN total_arr_delta < 0 AND coalesce(master_account_daily_arr.total_arr,0) != 0 THEN 'Contraction ARR'
            WHEN total_arr_delta < 0 AND coalesce(master_account_daily_arr.total_arr,0) = 0 THEN 'Churn ARR'
            ELSE NULL
        END as arr_type,
        sum(CASE WHEN account_new_arr THEN total_arr_delta ELSE 0 END) AS total_arr_new,
        sum(CASE WHEN not account_new_arr AND total_arr_delta > 0 THEN total_arr_delta ELSE 0 END) AS total_arr_expansion,
        sum(CASE WHEN total_arr_delta < 0 AND coalesce(master_account_daily_arr.total_arr,0) != 0 THEN total_arr_delta ELSE 0 END) AS total_arr_contraction,
        sum(CASE WHEN total_arr_delta < 0 AND coalesce(master_account_daily_arr.total_arr,0) = 0 THEN total_arr_delta ELSE 0 END) AS total_arr_churn,
        sum(total_arr_delta) AS total_arr_delta
    FROM {{ ref('master_account_monthly_arr_deltas') }}
        LEFT JOIN {{ ref('master_account_daily_arr') }}
            ON master_account_monthly_arr_deltas.master_account_sfid = master_account_daily_arr.master_account_sfid AND master_account_monthly_arr_deltas.month_end = master_account_daily_arr.day
    WHERE abs(total_arr_delta) > 0
    GROUP BY 1, 2, 3
)
select * from master_account_monthly_arr_deltas_by_type