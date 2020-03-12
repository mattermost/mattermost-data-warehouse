{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_arr_by_fy AS (
    SELECT 
        util.fiscal_year(day) AS fy,
        sum(total_arr) AS total_arr
    FROM  {{ ref('account_daily_arr') }}
    WHERE date_part('day', day + interval '1 day') = 1
        AND date_part('month', day) = 1
    GROUP BY 1
), arr_by_fy AS (
    SELECT *, util.fiscal_year(month) AS fy, min(month) AS min_month, max(month) AS max_month
    FROM {{ source('targets', 'arr_by_mo') }}
    WHERE date_part('month', month) = 1
    GROUP BY 1,2,3
), tva_arr_by_fy AS (
    SELECT
        'arr_by_fy' AS target_slug,
        arr_by_fy.fy,
        arr_by_fy.min_month AS period_first_day,
        arr_by_fy.max_month + interval '1 month' - interval '1 day' AS period_last_day,
        arr_by_fy.target,
        actual_arr_by_fy.total_arr AS actual,
        round((actual_arr_by_fy.total_arr/arr_by_fy.target),2) AS tva
    FROM arr_by_fy
    LEFT JOIN actual_arr_by_fy ON arr_by_fy.fy = actual_arr_by_fy.fy
)

SELECT * FROM tva_arr_by_fy