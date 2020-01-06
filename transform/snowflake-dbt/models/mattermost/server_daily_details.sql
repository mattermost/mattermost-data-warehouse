{{config({
    "materialized": "table"
  })
}}

WITH server_security_details AS (
    SELECT
  		id,
  		date AS day,
		max(user_count) AS user_count,
  		max(active_user_count) AS active_user_count
	FROM {{ ref('security') }}
    WHERE date >= '2016-01-01'
	GROUP BY id, day
), server_daily_details AS (
    SELECT 
        day, 
        account_sfid, 
        license_id, 
        server.user_id AS server_id,
        active_user_count
    FROM {{ source('mattermost2', 'server') }}
    LEFT JOIN {{ source('mattermost2', 'license') }} ON license.user_id = server.user_id
    LEFT JOIN {{ ref('license_overview') }} ON license_overview.licenseid = license.license_id
    LEFT JOIN server_security_details ON server_security_details.id = license.user_id
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM server_daily_details