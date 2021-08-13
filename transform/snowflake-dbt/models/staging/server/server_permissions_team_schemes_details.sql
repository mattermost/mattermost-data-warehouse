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
    FROM {{ source('mattermost2', 'permissions_team_schemes') }}
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
    FROM {{ source('mm_telemetry_prod', 'permissions_team_schemes') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_permissions_team_schemes_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , COALESCE(r.scheme_id, s.scheme_id)                                    AS scheme_id
           , MAX(COALESCE(r.channel_admin_permissions, s.channel_admin_permissions)) AS channel_admin_permissions
           , MAX(COALESCE(r.channel_guest_permissions, s.channel_guest_permissions)) AS channel_guest_permissions
           , MAX(COALESCE(r.channel_user_permissions, s.channel_user_permissions))  AS channel_user_permissions
           , MAX(COALESCE(r.team_admin_permissions, s.team_admin_permissions))    AS team_admin_permissions
           , MAX(COALESCE(r.team_count, s.team_count))                AS team_count
           , MAX(COALESCE(r.team_guest_permissions, s.team_guest_permissions))    AS team_guest_permissions
           , MAX(COALESCE(r.team_user_permissions, s.team_user_permissions))     AS team_user_permissions     
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)', 'COALESCE(r.scheme_id, s.scheme_id)']) }} AS id
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'permissions_team_schemes') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'permissions_team_schemes') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 3, 11)
SELECT *
FROM server_permissions_team_schemes_details