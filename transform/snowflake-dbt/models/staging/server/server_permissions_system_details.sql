{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp        AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'permissions_system_scheme') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'permissions_system_scheme') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_permissions_system_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , MAX(COALESCE(r.channel_admin_permissions, s.channel_admin_permissions)) AS channel_admin_permissions
           , MAX(COALESCE(r.channel_guest_permissions, s.channel_guest_permissions)) AS channel_guest_permissions
           , MAX(COALESCE(r.channel_user_permissions, s.channel_user_permissions))  AS channel_user_permissions
           , MAX(COALESCE(r.system_admin_permissions, s.system_admin_permissions))  AS system_admin_permissions
           , MAX(COALESCE(r.system_user_permissions, s.system_user_permissions))   AS system_user_permissions
           , MAX(COALESCE(r.team_admin_permissions, s.team_admin_permissions))    AS team_admin_permissions
           , MAX(COALESCE(r.team_guest_permissions, s.team_guest_permissions))    AS team_guest_permissions
           , MAX(COALESCE(r.team_user_permissions, s.team_user_permissions))     AS team_user_permissions           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , MAX(COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL))                   AS installation_id
            , MAX(COALESCE(r.system_user_manager_permissions_modified, NULL))            AS system_user_manager_permissions_modified
            , MAX(COALESCE(r.system_user_manager_permissions, NULL))            AS system_user_manager_permissions
            , MAX(COALESCE(r.system_user_manager_count, NULL))            AS system_user_manager_count
            , MAX(COALESCE(r.system_read_only_admin_permissions_modified, NULL))            AS system_read_only_admin_permissions_modified
            , MAX(COALESCE(r.system_read_only_admin_permissions, NULL))            AS system_read_only_admin_permissions
            , MAX(COALESCE(r.system_read_only_admin_count, NULL))            AS system_read_only_admin_count
            , MAX(COALESCE(r.system_manager_permissions_modified, NULL))            AS system_manager_permissions_modified
            , MAX(COALESCE(r.system_manager_permissions, NULL))            AS system_manager_permissions
            , MAX(COALESCE(r.system_manager_count, NULL))            AS system_manager_count

         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'permissions_system_scheme') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'permissions_system_scheme') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 11
         )
SELECT *
FROM server_permissions_system_details