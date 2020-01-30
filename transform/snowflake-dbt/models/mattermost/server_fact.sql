{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH server_fact AS (
    SELECT
        id AS server_id,
        account_sfid,
        license_id,
        MIN(date) AS first_active_date,
        MAX(date) AS last_active_date,
        MAX(active_user_count) AS max_active_user_count,
        MAX(CASE WHEN active_user_count > 0 THEN date ELSE null END) AS last_active_user_date
    FROM {{ ref('server_daily_details') }}
    GROUP BY 1, 2, 3
)
SELECT *
FROM server_fact