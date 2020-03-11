{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_tedas_7day_by_mo AS (
    SELECT
        date_trunc('month', server_daily_details.date) AS month,
        max(server_daily_details.date) as period_last_day,
        COUNT(DISTINCT CASE WHEN server_daily_details.date - server_fact.first_active_date >= 7 THEN server_daily_details.server_id ELSE NULL END) AS tedas_7day
    FROM {{ ref('server_daily_details') }}
    INNER JOIN {{ ref('server_fact') }} ON server_daily_details.server_id = server_fact.server_id
    WHERE (DATE_PART('day', server_daily_details.date + INTERVAL '1 day') = 1 OR (server_daily_details.date = CURRENT_DATE - INTERVAL '1 day'))
        AND server_daily_details.in_security
    GROUP BY server_daily_details.date
), tva_tedas_7day_by_mo AS (
    SELECT
        'tedas_7day_by_mo' as target_slug,
        tedas_7day_by_mo.month,
        actual_tedas_7day_by_mo.period_last_day,
        tedas_7day_by_mo.target,
        actual_tedas_7day_by_mo.tedas_7day as actual,
        round((actual_tedas_7day_by_mo.tedas_7day/tedas_7day_by_mo.target),2) as tva
    FROM {{ source('targets', 'tedas_7day_by_mo') }}
    LEFT JOIN actual_tedas_7day_by_mo ON tedas_7day_by_mo.month = actual_tedas_7day_by_mo.month
)

SELECT * FROM tva_tedas_7day_by_mo