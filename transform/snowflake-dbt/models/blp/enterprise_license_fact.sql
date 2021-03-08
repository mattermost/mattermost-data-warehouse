{{config({
    "materialized": "table",
    "schema": "blp"
  })
}}

WITH enterprise_license_fact AS (
    SELECT 
        licenses.account_sfid,
        licenses.license_id as licenseid,
        MIN(license_daily_details.start_date) AS start_date,
        MAX(licenses.server_expire_date_join::date) AS expiresat,
        MAX(license_daily_details.last_license_telemetry_date) AS last_license_telemetry_date,
        MAX(CASE WHEN license_daily_details.date = CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '1 days' THEN COALESCE(license_daily_details.license_users,license_daily_details.customer_users) ELSE NULL END)::int AS current_max_licensed_users,
        MAX(CASE WHEN license_daily_details.date = CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '1 days' THEN (license_daily_details.license_registered_users) ELSE NULL END)::int -
        COALESCE(MAX(CASE WHEN license_daily_details.date = CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '1 days' THEN (license_daily_details.license_registered_deactivated_users) ELSE NULL END)::int, 0) AS current_max_license_registered_users,
        AVG(license_daily_details.license_server_dau)::int AS current_rolling_7day_avg_dau,
        AVG(license_daily_details.license_server_mau)::int AS current_rolling_7day_avg_mau,
        MAX(CASE WHEN license_daily_details.date = CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '1 days' THEN license_daily_details.license_server_dau ELSE NULL END)::int AS current_max_license_server_dau,
        MAX(CASE WHEN license_daily_details.date = CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '1 days' THEN license_daily_details.license_server_mau ELSE NULL END)::int AS current_max_license_server_mau,
        ROUND(AVG(license_daily_details.servers),1) as current_avg_servers,
        MAX(CASE WHEN license_daily_details.date = CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '1 days' THEN license_daily_details.servers ELSE NULL END)::int as current_max_servers,
        MAX(COALESCE(month_ago.license_users,month_ago.customer_users))::int AS month_ago_max_licensed_users,
        AVG(month_ago.license_server_dau)::int AS month_ago_rolling_7day_avg_dau,
        AVG(month_ago.license_server_mau)::int AS month_ago_rolling_7day_avg_mau,
        MAX(CASE WHEN month_ago.date = CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '31 days' THEN month_ago.license_server_dau ELSE NULL END)::int AS month_ago_max_license_server_dau,
        MAX(CASE WHEN month_ago.date = CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '31 days' THEN month_ago.license_server_mau ELSE NULL END)::int AS month_ago_max_license_server_mau,
        ROUND(AVG(month_ago.servers),1) as month_ago_avg_servers,
        MAX(CASE WHEN month_ago.date = CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '31 days' THEN month_ago.servers ELSE NULL END)::int as month_ago_max_servers,
        MAX(CASE WHEN license_daily_details.date = CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '1 days' THEN license_daily_details.license_server_version ELSE NULL END) as current_license_server_version
    FROM {{ ref('licenses') }}
    LEFT JOIN {{ ref('license_daily_details') }} ON licenses.license_id = license_daily_details.license_id
        AND license_daily_details.date >= CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '8 days'
        AND license_daily_details.date < CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date
    LEFT JOIN {{ ref('license_daily_details') }} AS month_ago ON licenses.license_id = month_ago.license_id
        AND month_ago.date >= CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '38 days'
        AND month_ago.date < CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date - INTERVAL '30 days'
    LEFT JOIN {{ ref('account') }} ON account.sfid = licenses.account_sfid
    WHERE licenses.server_expire_date_join >= CONVERT_TIMEZONE('America/Los_Angeles',current_timestamp)::date AND NOT licenses.trial AND (account.arr_current__c > 0 or seats_licensed__c > 0)
    GROUP BY 1, 2
)

SELECT * FROM enterprise_license_fact