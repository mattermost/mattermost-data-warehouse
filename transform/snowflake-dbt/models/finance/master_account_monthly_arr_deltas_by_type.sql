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
            WHEN master_account_new_arr THEN 'New ARR'
            WHEN NOT master_account_new_arr AND month_starting_arr != 0 AND total_arr_delta > 0 THEN 'Expansion ARR'
            WHEN NOT master_account_new_arr AND month_starting_arr = 0 AND total_arr_delta > 0 THEN 'Resurrection ARR'
            WHEN total_arr_delta < 0 AND month_ending_arr != 0 THEN 'Contraction ARR'
            WHEN total_arr_delta < 0 AND month_ending_arr = 0 THEN 'Churn ARR'
            ELSE NULL
        END as arr_type,
        sum(CASE WHEN master_account_new_arr THEN total_arr_delta ELSE 0 END) AS total_arr_new,
        sum(CASE WHEN NOT master_account_new_arr AND month_starting_arr != 0 AND total_arr_delta > 0 THEN total_arr_delta ELSE 0 END) AS total_arr_expansion,
        sum(CASE WHEN NOT master_account_new_arr AND month_starting_arr = 0 AND total_arr_delta > 0 THEN total_arr_delta ELSE 0 END) AS total_arr_resurrection,
        sum(CASE WHEN total_arr_delta < 0 AND month_ending_arr != 0 THEN total_arr_delta ELSE 0 END) AS total_arr_contraction,
        sum(CASE WHEN total_arr_delta < 0 AND month_ending_arr = 0 THEN total_arr_delta ELSE 0 END) AS total_arr_churn,
        sum(total_arr_delta) AS total_arr_delta
    FROM {{ ref('master_account_monthly_arr_deltas') }}
    WHERE abs(total_arr_delta) > 0
    GROUP BY 1, 2, 3, 4
)
select * from master_account_monthly_arr_deltas_by_type