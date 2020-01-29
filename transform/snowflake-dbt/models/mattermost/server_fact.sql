{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

SELECT
    id AS server_id,
    account_sfid,
    license_id,
    min(date) as first_active_date,
    max(date) AS last_active_date,
    max(active_user_count) AS max_active_user_count,
    max(case when active_user_count > 0 then date else null end) AS last_active_user_date
FROM {{ ref('server_daily_details') }}
GROUP BY 1, 2, 3;