{{config({
    "materialized": "incremental",
    "schema": "focalboard",
    "tags":["nightly"],
    "unique_key":"focalboard_id"
  })
}}

WITH focalboard_fact AS (
    SELECT 
          focalboard_activity.user_id AS focalboard_id
        , MAX(server.server_id) AS instance_id
        , MIN(focalboard_activity.received_at) AS first_active
        , MAX(focalboard_activity.received_at) AS last_active
        , MAX(focalboard_activity.daily_active_users) AS daily_active_users_max
        , MAX(focalboard_activity.weekly_active_users) AS weekly_active_users_max
        , MAX(focalboard_activity.monthly_active_users) AS monthly_active_users_max
        , COUNT(DISTINCT CASE WHEN focalboard_activity.daily_active_users > 0 THEN focalboard_activity.timestamp::date ELSE NULL END) as days_active
        , MAX(focalboard_activity.received_at) as latest_received_at
    FROM {{ref('focalboard_activity')}}
    LEFT JOIN {{ ref('focalboard_server') }} server
      ON focalboard_activity.user_id = server.user_id
    GROUP BY 1
    {% if is_incremental() %}
    HAVING MAX(focalboard_activity.received_at) > (SELECT MAX(latest_received_at) FROM {{this}})
    {% endif %}
)

SELECT *
FROM focalboard_fact