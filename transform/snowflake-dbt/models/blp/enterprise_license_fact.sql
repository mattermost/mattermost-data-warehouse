{{config({
    "materialized": "table",
    "schema": "blp"
  })
}}

WITH enterprise_license_fact AS (
    SELECT 
        enterprise_license_mapping.account_sfid,
        enterprise_license_mapping.licenseid,
        license_daily_details.start_date,
        enterprise_license_mapping.expiresat::date as expiresat,
        license_daily_details.last_license_telemetry_date,
        MAX(COALESCE(license_daily_details.license_users,license_daily_details.customer_users))::int AS current_max_licensed_users,
        AVG(license_daily_details.license_server_dau)::int AS current_rolling_7day_avg_dau,
        AVG(license_daily_details.license_server_mau)::int AS current_rolling_7day_avg_mau,
        MAX(license_daily_details.license_server_dau)::int AS current_max_license_server_dau,
        MAX(license_daily_details.license_server_mau)::int AS current_max_license_server_mau,
        ROUND(AVG(license_daily_details.servers),1) as current_avg_servers,
        MAX(license_daily_details.servers)::int as current_max_servers,
        MAX(COALESCE(month_ago.license_users,month_ago.customer_users))::int AS month_ago_max_licensed_users,
        AVG(month_ago.license_server_dau)::int AS month_ago_rolling_7day_avg_dau,
        AVG(month_ago.license_server_mau)::int AS month_ago_rolling_7day_avg_mau,
        MAX(month_ago.license_server_dau)::int AS month_ago_max_license_server_dau,
        MAX(month_ago.license_server_mau)::int AS month_ago_max_license_server_mau,
        ROUND(AVG(month_ago.servers),1) as month_ago_avg_servers,
        MAX(month_ago.servers)::int as month_ago_max_servers
    FROM {{ ref('enterprise_license_mapping') }}
    LEFT JOIN {{ ref('license_daily_details') }} ON enterprise_license_mapping.licenseid = license_daily_details.license_id
        AND license_daily_details.date >= CURRENT_DATE - INTERVAL '8 days'
        AND license_daily_details.date < CURRENT_DATE
    LEFT JOIN {{ ref('license_daily_details') }} AS month_ago ON enterprise_license_mapping.licenseid = month_ago.license_id
        AND month_ago.date >= CURRENT_DATE - INTERVAL '38 days'
        AND month_ago.date < CURRENT_DATE - INTERVAL '30 days'
    WHERE enterprise_license_mapping.expiresat >= CURRENT_DATE
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM enterprise_license_fact