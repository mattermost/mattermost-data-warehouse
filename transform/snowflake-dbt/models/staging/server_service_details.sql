{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_service') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_service_details AS (
         SELECT
             timestamp::DATE                                              AS date
           , s.user_id                                                    AS server_id
           , MAX(allow_cookies_for_subdomains)                            AS allow_cookies_for_subdomains
           , MAX(allow_edit_post)                                         AS allow_edit_post
           , MAX(close_unused_direct_messages)                            AS close_unused_direct_messages
           , MAX(cluster_log_timeout_milliseconds)                        AS cluster_log_timeout_milliseconds
           , MAX(connection_security)                                     AS connection_security
           , MAX(cors_allow_credentials)                                  AS cors_allow_credentials
           , MAX(cors_debug)                                              AS cors_debug
           , MAX(custom_service_terms_enabled)                            AS custom_service_terms_enabled
           , MAX(disable_bots_when_owner_is_deactivated)                  AS disable_bots_when_owner_is_deactivated
           , MAX(disable_legacy_mfa)                                      AS disable_legacy_mfa
           , MAX(enable_apiv3)                                            AS enable_apiv3
           , MAX(enable_api_team_deletion)                                AS enable_api_team_deletion
           , MAX(enable_bot_account_creation)                             AS enable_bot_account_creation
           , MAX(enable_channel_viewed_messages)                          AS enable_channel_viewed_messages
           , MAX(enable_commands)                                         AS enable_commands
           , MAX(enable_custom_emoji)                                     AS enable_custom_emoji
           , MAX(enable_developer)                                        AS enable_developer
           , MAX(enable_email_invitations)                                AS enable_email_invitations
           , MAX(enable_emoji_picker)                                     AS enable_emoji_picker
           , MAX(enable_gif_picker)                                       AS enable_gif_picker
           , MAX(enable_incoming_webhooks)                                AS enable_incoming_webhooks
           , MAX(enable_insecure_outgoing_connections)                    AS enable_insecure_outgoing_connections
           , MAX(enable_latex)                                            AS enable_latex
           , MAX(enable_multifactor_authentication)                       AS enable_multifactor_authentication
           , MAX(enable_oauth_service_provider)                           AS enable_oauth_service_provider
           , MAX(enable_only_admin_integrations)                          AS enable_only_admin_integrations
           , MAX(enable_outgoing_webhooks)                                AS enable_outgoing_webhooks
           , MAX(enable_post_icon_override)                               AS enable_post_icon_override
           , MAX(enable_post_search)                                      AS enable_post_search
           , MAX(enable_post_username_override)                           AS enable_post_username_override
           , MAX(enable_preview_features)                                 AS enable_preview_features
           , MAX(enable_security_fix_alert)                               AS enable_security_fix_alert
           , MAX(enable_svgs)                                             AS enable_svgs
           , MAX(enable_testing)                                          AS enable_testing
           , MAX(enable_tutorial)                                         AS enable_tutorial
           , MAX(enable_user_access_tokens)                               AS enable_user_access_tokens
           , MAX(enable_user_statuses)                                    AS enable_user_statuses
           , MAX(enable_user_typing_messages)                             AS enable_user_typing_messages
           , MAX(enforce_multifactor_authentication)                      AS enforce_multifactor_authentication
           , MAX(experimental_channel_organization)                       AS experimental_channel_organization
           , MAX(experimental_enable_authentication_transfer)             AS experimental_enable_authentication_transfer
           , MAX(experimental_enable_default_channel_leave_join_messages) AS experimental_enable_default_channel_leave_join_messages
           , MAX(experimental_enable_hardened_mode)                       AS experimental_enable_hardened_mode
           , MAX(experimental_group_unread_channels)                      AS experimental_group_unread_channels
           , MAX(experimental_ldap_group_sync)                            AS experimental_ldap_group_sync
           , MAX(experimental_limit_client_config)                        AS experimental_limit_client_config
           , MAX(experimental_strict_csrf_enforcement)                    AS experimental_strict_csrf_enforcement
           , MAX(forward_80_to_443)                                       AS forward_80_to_443
           , MAX(gfycat_api_key)                                          AS gfycat_api_key
           , MAX(gfycat_api_secret)                                       AS gfycat_api_secret
           , MAX(isdefault_allowed_untrusted_internal_connections)        AS isdefault_allowed_untrusted_internal_connections
           , MAX(isdefault_allowed_untrusted_inteznal_connections)        AS isdefault_allowed_untrusted_inteznal_connections
           , MAX(isdefault_allow_cors_from)                               AS isdefault_allow_cors_from
           , MAX(isdefault_cors_exposed_headers)                          AS isdefault_cors_exposed_headers
           , MAX(isdefault_google_developer_key)                          AS isdefault_google_developer_key
           , MAX(isdefault_image_proxy_options)                           AS isdefault_image_proxy_options
           , MAX(isdefault_image_proxy_type)                              AS isdefault_image_proxy_type
           , MAX(isdefault_image_proxy_url)                               AS isdefault_image_proxy_url
           , MAX(isdefault_read_timeout)                                  AS isdefault_read_timeout
           , MAX(isdefault_site_url)                                      AS isdefault_site_url
           , MAX(isdefault_tls_cert_file)                                 AS isdefault_tls_cert_file
           , MAX(isdefault_tls_key_file)                                  AS isdefault_tls_key_file
           , MAX(isdefault_write_timeout)                                 AS isdefault_write_timeout
           , MAX(maximum_login_attempts)                                  AS maximum_login_attempts
           , MAX(minimum_hashtag_length)                                  AS minimum_hashtag_length
           , MAX(post_edit_time_limit)                                    AS post_edit_time_limit
           , MAX(restrict_custom_emoji_creation)                          AS restrict_custom_emoji_creation
           , MAX(restrict_post_delete)                                    AS restrict_post_delete
           , MAX(session_cache_in_minutes)                                AS session_cache_in_minutes
           , MAX(session_idle_timeout_in_minutes)                         AS session_idle_timeout_in_minutes
           , MAX(session_length_mobile_in_days)                           AS session_length_mobile_in_days
           , MAX(session_length_sso_in_days)                              AS session_length_sso_in_days
           , MAX(session_length_web_in_days)                              AS session_length_web_in_days
           , MAX(time_between_user_typing_updates_milliseconds)           AS time_between_user_typing_updates_milliseconds
           , MAX(tls_strict_transport)                                    AS tls_strict_transport
           , MAX(uses_letsencrypt)                                        AS uses_letsencrypt
           , MAX(websocket_url)                                           AS websocket_url
           , MAX(web_server_mode)                                         AS web_server_mode
         FROM {{ source('staging_config', 'config_service') }} s
              JOIN max_timestamp         mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_service_details