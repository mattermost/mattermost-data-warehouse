{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp               AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_client_requirements') }}
    WHERE timestamp::DATE <= CURRENT_DATE - INTERVAL '1 DAY'
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_client_requirements_details AS (
         SELECT
             timestamp::DATE                           AS date
           , ccr.user_id                               AS server_id
           , MAX(allow_edit_post)                      AS allow_edit_post
           , MAX(android_latest_version)               AS android_latest_version
           , MAX(android_min_version)                  AS android_min_version
           , MAX(cluster_log_timeout_milliseconds)     AS cluster_log_timeout_milliseconds
           , MAX(desktop_latest_version)               AS desktop_latest_version
           , MAX(desktop_min_version)                  AS desktop_min_version
           , MAX(enable_apiv3)                         AS enable_apiv3
           , MAX(enable_channel_viewed_messages)       AS enable_channel_viewed_messages
           , MAX(enable_commands)                      AS enable_commands
           , MAX(enable_custom_emoji)                  AS enable_custom_emoji
           , MAX(enable_developer)                     AS enable_developer
           , MAX(enable_emoji_picker)                  AS enable_emoji_picker
           , MAX(enable_incoming_webhooks)             AS enable_incoming_webhooks
           , MAX(enable_insecure_outgoing_connections) AS enable_insecure_outgoing_connections
           , MAX(enable_multifactor_authentication)    AS enable_multifactor_authentication
           , MAX(enable_oauth_service_provider)        AS enable_oauth_service_provider
           , MAX(enable_only_admin_integrations)       AS enable_only_admin_integrations
           , MAX(ios_latest_version)                   AS ios_latest_version
           , MAX(ios_min_version)                      AS ios_min_version
         FROM {{ source('mattermost2', 'config_client_requirements') }} ccr
              JOIN max_timestamp              mt
                   ON ccr.user_id = mt.user_id
                       AND mt.max_timestamp = ccr.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_client_requirements_details