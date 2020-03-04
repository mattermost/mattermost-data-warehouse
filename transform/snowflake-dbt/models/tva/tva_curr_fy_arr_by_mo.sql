{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH actual_arr_by_mo AS (
    SELECT 
        date_trunc('month', day) AS month,
        sum(total_arr) AS total_arr
    FROM {{ ref('finance', 'account_daily_arr') }}
    WHERE date_part('day', day + interval '1 day') = 1
    GROUP BY 1
), curr_fy_arr_by_mo AS (
    SELECT
        month::date,
        target,
        total_arr,
        round((total_arr/target),2) as tva
    FROM {{ source('target', 'curr_fy_arr_by_mo') }}
    LEFT JOIN actual_arr_by_mo ON curr_fy_arr_by_mo.month = actual_arr_by_mo.month
)

SELECT * FROM tva_curr_fy_arr_by_mo