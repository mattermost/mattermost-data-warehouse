{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_arr_by_mo AS (
    SELECT 
        date_trunc('month', day) AS month,
        sum(total_arr) AS total_arr
    FROM  {{ ref('account_daily_arr') }}
    WHERE date_part('day', day + interval '1 day') = 1
    GROUP BY 1
), tva_arr_by_mo AS (
    SELECT
        'arr_by_mo' AS target_slug,
        arr_by_mo.month,
        arr_by_mo.month AS period_first_day,
        arr_by_mo.month + interval '1 month' - interval '1 day' AS period_last_day,
        arr_by_mo.target,
        actual_arr_by_mo.total_arr AS actual,
        round((actual_arr_by_mo.total_arr/arr_by_mo.target),2) AS tva
    FROM {{ source('targets', 'arr_by_mo') }}
    LEFT JOIN actual_arr_by_mo ON arr_by_mo.month = actual_arr_by_mo.month
)

SELECT * FROM tva_arr_by_mo