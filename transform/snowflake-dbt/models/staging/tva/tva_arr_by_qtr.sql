{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_arr_by_qtr AS (
    SELECT 
        util.fiscal_year(day)|| '-' || util.fiscal_quarter(day) AS qtr,
        sum(total_arr) AS total_arr
    FROM  {{ ref('account_daily_arr') }}
    WHERE date_part('day', day + interval '1 day') = 1
        AND date_part('month', day) in (1,4,7,10)
    GROUP BY 1
), arr_by_qtr AS (
    SELECT *, util.fiscal_year(month)|| '-' || util.fiscal_quarter(month) AS qtr, max(month) AS max_month
    FROM {{ source('targets', 'arr_by_mo') }}
    WHERE date_part('month', month) in (1,4,7,10)
    GROUP BY 1,2,3
), tva_arr_by_qtr AS (
    SELECT
        'arr_by_qtr' AS target_slug,
        arr_by_qtr.qtr,
        arr_by_qtr.max_month - interval '2 months' AS period_first_day,
        arr_by_qtr.max_month + interval '1 month' - interval '1 day' AS period_last_day,
        arr_by_qtr.target,
        actual_arr_by_qtr.total_arr AS actual,
        round((actual_arr_by_qtr.total_arr/arr_by_qtr.target),3) AS tva
    FROM arr_by_qtr
    LEFT JOIN actual_arr_by_qtr ON arr_by_qtr.qtr = actual_arr_by_qtr.qtr
)

SELECT * FROM tva_arr_by_qtr