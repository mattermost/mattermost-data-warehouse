{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_arr_by_mo AS (
    SELECT 
        date_trunc('month', day) AS month,
        max(day) as period_last_day,
        sum(total_arr) AS total_arr
    FROM  {{ ref('account_daily_arr') }}
    WHERE date_part('day', day + interval '1 day') = 1
    GROUP BY 1
), tva_arr_by_mo AS (
    SELECT
        'arr_by_mo' as target_slug,
        arr_by_mo.month,
        actual_arr_by_mo.period_last_day,
        arr_by_mo.target,
        actual_arr_by_mo.total_arr as actual,
        round((actual_arr_by_mo.total_arr/arr_by_mo.target),2) as tva
    FROM {{ source('targets', 'arr_by_mo') }}
    LEFT JOIN actual_arr_by_mo ON arr_by_mo.month = actual_arr_by_mo.month
)

SELECT * FROM tva_arr_by_mo