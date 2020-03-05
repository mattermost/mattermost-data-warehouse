{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH actual_tedas_7day_by_mo AS (
    SELECT
        date_trunc('month', server_daily_details.date) AS month,
        COUNT(DISTINCT CASE WHEN server_daily_details.date - server_fact.first_active_date >= 7 THEN server_daily_details.server_id ELSE NULL END) AS tedas_7day
    FROM {{ ref('server_daily_details') }}
    INNER JOIN {{ ref('server_fact') }} ON server_daily_details.server_id = server_fact.server_id
    WHERE DATE_PART('day', server_daily_details.date + INTERVAL '1 day') = 1 
        AND server_daily_details.in_security
    GROUP BY server_daily_details.date
), tva_curr_fy_tedas_7day_by_mo AS (
    SELECT
        curr_fy_tedas_7day_by_mo.month,
        curr_fy_tedas_7day_by_mo.target,
        actual_tedas_7day_by_mo.tedas_7day as actual,
        round((actual_tedas_7day_by_mo.tedas_7day/curr_fy_tedas_7day_by_mo.target),2) as tva
    FROM {{ source('targets', 'curr_fy_tedas_7day_by_mo') }}
    LEFT JOIN actual_tedas_7day_by_mo ON curr_fy_tedas_7day_by_mo.month = actual_tedas_7day_by_mo.month
)

SELECT * FROM tva_curr_fy_tedas_7day_by_mo