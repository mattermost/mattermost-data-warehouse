{{config({
    "materialized": "table",
    "schema": "blp"
  })
}}

WITH enterprise_license_fact AS (
    SELECT 
        licenses.account_sfid,
        licenses.license_id as licenseid,
        license_daily_details.start_date,
        licenses.server_expire_date_join::date AS expiresat,
        MAX(license_daily_details.last_license_telemetry_date) AS last_license_telemetry_date,
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
        MAX(month_ago.servers)::int as month_ago_max_servers,
        MAX(license_daily_details.license_server_version) as current_license_server_version
    FROM {{ ref('licenses') }}
    LEFT JOIN {{ ref('license_daily_details') }} ON licenses.license_id = license_daily_details.license_id
        AND license_daily_details.date >= CURRENT_DATE - INTERVAL '8 days'
        AND license_daily_details.date < CURRENT_DATE
    LEFT JOIN {{ ref('license_daily_details') }} AS month_ago ON licenses.license_id = month_ago.license_id
        AND month_ago.date >= CURRENT_DATE - INTERVAL '38 days'
        AND month_ago.date < CURRENT_DATE - INTERVAL '30 days'
    LEFT JOIN {{ source('orgm','account') }} ON account.sfid = licenses.account_sfid
    WHERE licenses.server_expire_date_join >= CURRENT_DATE AND NOT licenses.trial AND (account.arr_current__c > 0 or seats_licensed__c > 0)
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM enterprise_license_fact