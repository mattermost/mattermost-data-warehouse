{{config({
    "materialized": 'table',
    "schema": "tva"
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
    SELECT util.fiscal_year(month) AS fy, *
    FROM {{ source('targets', 'arr_by_mo') }}
    WHERE date_part('month', month) = 1
), tva_arr_by_fy AS (
    SELECT
        'arr_by_fy' as target_slug,
        arr_by_fy.fy,
        arr_by_fy.target,
        actual_arr_by_fy.total_arr as actual,
        round((actual_arr_by_fy.total_arr/arr_by_fy.target),2) as tva
    FROM arr_by_fy
    LEFT JOIN actual_arr_by_fy ON arr_by_fy.fy = actual_arr_by_fy.fy
)

SELECT * FROM tva_arr_by_fy