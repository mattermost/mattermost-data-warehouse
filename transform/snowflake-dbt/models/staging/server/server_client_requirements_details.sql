{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp               AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_client_requirements') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp               AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_client_requirements') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_client_requirements_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::date)              AS date
           , COALESCE(s.user_id, r.user_id)                              AS server_id
           , MAX(COALESCE(s.allow_edit_post, NULL))                      AS allow_edit_post
           , MAX(COALESCE(s.android_latest_version, r.android_latest_version))               AS android_latest_version
           , MAX(COALESCE(s.android_min_version, r.android_min_version))                  AS android_min_version
           , MAX(COALESCE(s.cluster_log_timeout_milliseconds, NULL))     AS cluster_log_timeout_milliseconds
           , MAX(COALESCE(s.desktop_latest_version, r.desktop_latest_version))               AS desktop_latest_version
           , MAX(COALESCE(s.desktop_min_version, r.desktop_min_version))                  AS desktop_min_version
           , MAX(COALESCE(s.enable_apiv3, NULL))                         AS enable_apiv3
           , MAX(COALESCE(s.enable_channel_viewed_messages, NULL))       AS enable_channel_viewed_messages
           , MAX(COALESCE(s.enable_commands, NULL))                      AS enable_commands
           , MAX(COALESCE(s.enable_custom_emoji, NULL))                  AS enable_custom_emoji
           , MAX(COALESCE(s.enable_developer, NULL))                     AS enable_developer
           , MAX(COALESCE(s.enable_emoji_picker, NULL))                  AS enable_emoji_picker
           , MAX(COALESCE(s.enable_incoming_webhooks, NULL))             AS enable_incoming_webhooks
           , MAX(COALESCE(s.enable_insecure_outgoing_connections, NULL)) AS enable_insecure_outgoing_connections
           , MAX(COALESCE(s.enable_multifactor_authentication, NULL))    AS enable_multifactor_authentication
           , MAX(COALESCE(s.enable_oauth_service_provider, NULL))        AS enable_oauth_service_provider
           , MAX(COALESCE(s.enable_only_admin_integrations, NULL))       AS enable_only_admin_integrations
           , MAX(COALESCE(s.ios_latest_version, r.ios_latest_version))                   AS ios_latest_version
           , MAX(COALESCE(s.ios_min_version, r.ios_min_version))                      AS ios_min_version
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_client_requirements') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_client_requirements') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 22, 23
     )
SELECT *
FROM server_client_requirements_details