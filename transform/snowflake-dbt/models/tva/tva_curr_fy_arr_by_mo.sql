{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH actual_arr_by_mo AS (
    SELECT 
        date_trunc('month', day) AS month,
        sum(total_arr) AS total_arr
    FROM  {{ ref('account_daily_arr') }}
    WHERE date_part('day', day + interval '1 day') = 1
    GROUP BY 1
), tva_curr_fy_arr_by_mo AS (
    SELECT
        curr_fy_arr_by_mo.month,
        curr_fy_arr_by_mo.target,
        actual_arr_by_mo.total_arr,
        round((actual_arr_by_mo.total_arr/curr_fy_arr_by_mo.target),2) as tva
    FROM {{ source('targets', 'curr_fy_arr_by_mo') }}
    LEFT JOIN actual_arr_by_mo ON curr_fy_arr_by_mo.month = actual_arr_by_mo.month
)

SELECT * FROM tva_curr_fy_arr_by_mo