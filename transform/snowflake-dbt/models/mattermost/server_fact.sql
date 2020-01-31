{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH server_details AS (
    SELECT
        id AS server_id,
        MIN(date) AS first_active_date,
        MAX(date) AS last_active_date,
        MAX(active_user_count) AS max_active_user_count,
        MAX(CASE WHEN active_user_count > 0 THEN date ELSE null END) AS last_active_user_date
    FROM {{ ref('server_daily_details') }}
    GROUP BY 1
),
server_fact AS (
    SELECT
        server_details.server_id,
        server_daily_details.account_sfid,
        server_daily_details.license_id,
        server_details.first_active_date,
        server_details.last_active_date,
        server_details.max_active_user_count,
        server_details.last_active_user_date
    FROM server_details 
    JOIN {{ ref('server_daily_details') }}
        ON server_details.server_id = server_daily_details.id
        AND server_details.last_active_date = server_daily_details.date
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT *
FROM server_fact