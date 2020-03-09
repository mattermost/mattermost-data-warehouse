{{config({
    "materialized": 'table',
    "schema": "tva"
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
        arr_exp_by_mo.month,
        arr_exp_by_mo.target,
        actual_arr_exp_by_mo.total_arr as actual,
        round((actual_arr_exp_by_mo.total_arr/arr_exp_by_mo.target),2) as tva
    FROM {{ source('targets', 'arr_exp_by_mo') }}
    LEFT JOIN actual_arr_exp_by_mo ON arr_exp_by_mo.month = actual_arr_exp_by_mo.month
)

SELECT * FROM tva_arr_exp_by_mo