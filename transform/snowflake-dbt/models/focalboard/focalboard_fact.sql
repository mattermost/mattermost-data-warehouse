{{config({
    "materialized": "incremental",
    "schema": "focalboard",
    "tags":"hourly",
    "unique_key":"instance_id"
  })
}}

WITH focalboard_fact AS (
    SELECT 
          user_id AS focalboard_id
        , server.server_id AS instance_id
        , MIN(timestamp) AS first_active
        , MAX(timestamp) AS last_active
        , MAX(daily_active_users) AS daily_active_users_max
        , MAX(weekly_active_users) AS weekly_active_users_max
        , MAX(monthly_active_users) AS monthly_active_users_max
        , COUNT(DISTINCT CASE WHEN daily_active_users > 0 THEN timestamp::date ELSE NULL END) as days_active
    FROM {{ref('focalboard_activity')}}
    LEFT JOIN {{ ref('focalboard_server') }} server
      ON focalboard_activity.user_id = server.user_id
    GROUP BY 1
    {% if is_incremental() %}
    HAVING MAX(received_at) >= (SELECT MAX(received_at) FROM {{this}})
    {% endif %}
)

SELECT *
FROM focalboard_fact