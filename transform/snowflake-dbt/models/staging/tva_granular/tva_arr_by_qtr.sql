{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH actual_arr_by_qtr AS (
    SELECT 
        util.fiscal_year(day)|| '-' || util.fiscal_quarter(day) AS qtr,
        max(day) as period_last_day,
        sum(total_arr) AS total_arr
    FROM  {{ ref('account_daily_arr') }}
    WHERE date_part('day', day + interval '1 day') = 1
        AND date_part('month', day) in (1,4,7,10)
    GROUP BY 1
), arr_by_qtr AS (
    SELECT util.fiscal_year(month)|| '-' || util.fiscal_quarter(month) AS qtr, *
    FROM {{ source('targets', 'arr_by_mo') }}
    WHERE date_part('month', month) in (1,4,7,10)
), tva_arr_by_qtr AS (
    SELECT
        'arr_by_qtr' as target_slug,
        arr_by_qtr.qtr,
        actual_arr_by_qtr.period_last_day,
        arr_by_qtr.target,
        actual_arr_by_qtr.total_arr as actual,
        round((actual_arr_by_qtr.total_arr/arr_by_qtr.target),2) as tva
    FROM arr_by_qtr
    LEFT JOIN actual_arr_by_qtr ON arr_by_qtr.qtr = actual_arr_by_qtr.qtr
)

SELECT * FROM tva_arr_by_qtr