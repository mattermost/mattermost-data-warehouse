{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp               AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , max(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_client_requirements') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_client_requirements_details AS (
         SELECT
             timestamp::DATE                  AS date
           , ccr.user_id                       AS server_id
           , max(enable_commands)               AS enabled_commands
           , max(enable_oauth_service_provider)      AS enable_oauth_service_provider
           , max(enable_emoji_picker) AS enable_emoji_picker
           , max(enable_developer)      AS enable_developer
              , max(allow_edit_post)      AS allow_edit_post
         , max(enable_apiv3)      AS enable_apiv3
         , max(enable_custom_emoji)      AS enable_custom_emoji
         , max(enable_insecure_outgoing_connections)      AS enable_insecure_outgoing_connections
         , max(enable_channel_viewed_messages)      AS enable_channel_viewed_messages
         , max(enable_incoming_webhooks)      AS enable_incoming_webhooks
         , max(enable_multifactor_authentication)      AS enable_multifactor_authentication
         , max(enable_only_admin_integrations)      AS enable_only_admin_integrations
         FROM {{ source('staging_config', 'config_client_requirements') }} ccr
              JOIN max_timestamp              mt
                   ON ccr.user_id = mt.user_id
                       AND mt.max_timestamp = ccr.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_client_requirements_details