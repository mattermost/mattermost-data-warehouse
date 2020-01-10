{{config({
    "materialized": 'incremental',
    "schema": "mattermost"
  })
}}

WITH server_security_details AS (
    SELECT
  		id,
  		date AS day,
		MAX(user_count) AS user_count,
  		MAX(active_user_count) AS active_user_count
	FROM {{ ref('security') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE date > (SELECT MAX(day) FROM {{ this }})

    {% endif %}

	GROUP BY 1, 2
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

