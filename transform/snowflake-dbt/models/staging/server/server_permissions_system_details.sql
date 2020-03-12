{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp          AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'permissions_system_scheme') }}
    WHERE timestamp::DATE <= CURRENT_DATE - INTERVAL '1 DAY'
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_permissions_system_details AS (
         SELECT
             p.timestamp::DATE              AS date
           , p.user_id
           , MAX(channel_admin_permissions) AS channel_admin_permissions
           , MAX(channel_guest_permissions) AS channel_guest_permissions
           , MAX(channel_user_permissions)  AS channel_user_permissions
           , MAX(system_admin_permissions)  AS system_admin_permissions
           , MAX(system_user_permissions)   AS system_user_permissions
           , MAX(team_admin_permissions)    AS team_admin_permissions
           , MAX(team_guest_permissions)    AS team_guest_permissions
           , MAX(team_user_permissions)     AS team_user_permissions
         FROM {{ source('mattermost2', 'permissions_system_scheme') }} p
              JOIN max_timestamp                        mt
                   ON p.user_id = mt.user_id
                       AND p.timestamp = mt.max_timestamp
         GROUP BY 1, 2)
SELECT *
FROM server_permissions_system_details