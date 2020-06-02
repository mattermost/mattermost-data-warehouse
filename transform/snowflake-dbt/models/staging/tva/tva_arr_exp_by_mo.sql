{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_arr_exp_by_mo AS (
    SELECT 
        account_monthly_arr_deltas_by_type.month_start AS month,
        SUM(account_monthly_arr_deltas_by_type.total_arr_expansion) AS total_arr
    FROM  {{ ref('account_monthly_arr_deltas_by_type') }}
    WHERE account_monthly_arr_deltas_by_type.month_start < CURRENT_DATE
    GROUP BY 1
), tva_arr_exp_by_mo AS (
    SELECT
        'arr_exp_by_mo' AS target_slug,
        arr_exp_by_mo.month,
        arr_exp_by_mo.month AS period_first_day,
        arr_exp_by_mo.month + interval '1 month' - interval '1 day' AS period_last_day,
        arr_exp_by_mo.target,
        actual_arr_exp_by_mo.total_arr AS actual,
        round((actual_arr_exp_by_mo.total_arr/arr_exp_by_mo.target),3) AS tva
    FROM {{ source('targets', 'arr_exp_by_mo') }}
    LEFT JOIN actual_arr_exp_by_mo ON arr_exp_by_mo.month = actual_arr_exp_by_mo.month
)

SELECT * FROM tva_arr_exp_by_mo