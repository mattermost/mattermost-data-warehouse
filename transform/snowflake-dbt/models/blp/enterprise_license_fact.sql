{{config({
    "materialized": "table",
    "schema": "blp"
  })
}}

WITH enterprise_license_fact AS (
    SELECT 
        account_sfid,
        licenseid,
        expiresat,
        last_license_telemetry_date,
        SUM(license_daily_details.license_server_dau)/COUNT(license_daily_details.date) AS current_rolling_7day_avg_dau,
        SUM(license_daily_details.license_server_mau)/COUNT(license_daily_details.date) AS current_rolling_7day_avg_mau,
        MAX(license_daily_details.license_server_dau) AS current_max_license_server_dau,
        MAX(license_daily_details.license_server_mau) AS current_max_license_server_mau,
        SUM(license_daily_details.servers)/COUNT(license_daily_details.date) as current_ago_avg_servers,
        MAX(license_daily_details.servers) as current_ago_max_servers,
        SUM(month_ago.license_server_dau)/COUNT(month_ago.date) AS month_ago_rolling_7day_avg_dau,
        SUM(month_ago.license_server_mau)/COUNT(month_ago.date) AS month_ago_rolling_7day_avg_mau,
        MAX(month_ago.license_server_dau) AS month_ago_max_license_server_dau,
        MAX(month_ago.license_server_mau) AS month_ago_max_license_server_mau,
        SUM(month_ago.servers)/COUNT(month_ago.date) as month_ago_avg_servers,
        MAX(month_ago.servers) as month_ago_max_servers
    FROM {{ ref('enterprise_license_mapping') }}
    LEFT JOIN {{ ref('license_daily_details') }} ON enterprise_license_mapping.licenseid = license_daily_details.license_id
        AND license_daily_details.date >= CURRENT_DATE - INTERVAL '8 days'
        AND license_daily_details.date < CURRENT_DATE
    LEFT JOIN {{ ref('license_daily_details') }} AS month_ago ON enterprise_license_mapping.licenseid = month_ago.license_id
        AND month_ago.date >= CURRENT_DATE - INTERVAL '38 days'
        AND month_ago.date < CURRENT_DATE - INTERVAL '30 days'
    WHERE enterprise_license_mapping.expiresat >= CURRENT_DATE
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM enterprise_license_fact