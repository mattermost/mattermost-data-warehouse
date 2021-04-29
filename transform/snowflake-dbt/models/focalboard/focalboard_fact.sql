{{config({
    "materialized": "incremental",
    "schema": "focalboard",
    "tags":"hourly",
    "unique_key":"server_id"
  })
}}

WITH focalboard_fact AS (
    SELECT 
        server_id
        , MIN(timestamp) AS first_active
        , MAX(timestamp) AS last_active
        , MAX(daily_active_users) AS daily_active_users_max
        , MAX(weekly_active_users) AS weekly_active_users_max
        , MAX(monthly_active_users) AS monthly_active_users_max
    FROM {{ref('focal_board_activity')}}
    GROUP BY 1
    {% if is_incremental() %}
    HAVING MAX(timestamp) >= (SELECT MAX(last_active) FROM {{this}})
    {% endif %}
)

SELECT *
FROM focalboard_fact