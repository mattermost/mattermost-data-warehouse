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
    FROM {{ source('mattermost2', 'config_service') }}
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
    FROM {{ source('mm_telemetry_prod', 'config_service') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_service_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , MAX(COALESCE(r.allow_cookies_for_subdomains, s.allow_cookies_for_subdomains))                            AS allow_cookies_for_subdomains
           , MAX(COALESCE(r.allow_edit_post, s.allow_edit_post))                                         AS allow_edit_post
           , MAX(COALESCE(r.close_unused_direct_messages, s.close_unused_direct_messages))                            AS close_unused_direct_messages
           , MAX(COALESCE(r.cluster_log_timeout_milliseconds, s.cluster_log_timeout_milliseconds))                        AS cluster_log_timeout_milliseconds
           , MAX(COALESCE(r.connection_security, s.connection_security))                                     AS connection_security
           , MAX(COALESCE(r.cors_allow_credentials, s.cors_allow_credentials))                                  AS cors_allow_credentials
           , MAX(COALESCE(r.cors_debug, s.cors_debug))                                              AS cors_debug
           , MAX(COALESCE(s.custom_service_terms_enabled, NULL ))                            AS custom_service_terms_enabled
           , MAX(COALESCE(r.disable_bots_when_owner_is_deactivated, s.disable_bots_when_owner_is_deactivated))                  AS disable_bots_when_owner_is_deactivated
           , MAX(COALESCE(r.disable_legacy_mfa, s.disable_legacy_mfa))                                      AS disable_legacy_mfa
           , MAX(COALESCE(s.enable_apiv3, NULL ))                                            AS enable_apiv3
           , MAX(COALESCE(r.enable_api_team_deletion, s.enable_api_team_deletion))                                AS enable_api_team_deletion
           , MAX(COALESCE(r.enable_bot_account_creation, s.enable_bot_account_creation))                             AS enable_bot_account_creation
           , MAX(COALESCE(r.enable_channel_viewed_messages, s.enable_channel_viewed_messages))                          AS enable_channel_viewed_messages
           , MAX(COALESCE(r.enable_commands, s.enable_commands))                                         AS enable_commands
           , MAX(COALESCE(r.enable_custom_emoji, s.enable_custom_emoji))                                     AS enable_custom_emoji
           , MAX(COALESCE(r.enable_developer, s.enable_developer))                                        AS enable_developer
           , MAX(COALESCE(r.enable_email_invitations, s.enable_email_invitations))                                AS enable_email_invitations
           , MAX(COALESCE(r.enable_emoji_picker, s.enable_emoji_picker))                                     AS enable_emoji_picker
           , MAX(COALESCE(r.enable_gif_picker, s.enable_gif_picker))                                       AS enable_gif_picker
           , MAX(COALESCE(r.enable_incoming_webhooks, s.enable_incoming_webhooks))                                AS enable_incoming_webhooks
           , MAX(COALESCE(r.enable_insecure_outgoing_connections, s.enable_insecure_outgoing_connections))                    AS enable_insecure_outgoing_connections
           , MAX(COALESCE(r.enable_latex, s.enable_latex))                                            AS enable_latex
           , MAX(COALESCE(r.enable_multifactor_authentication, s.enable_multifactor_authentication))                       AS enable_multifactor_authentication
           , MAX(COALESCE(r.enable_oauth_service_provider, s.enable_oauth_service_provider))                           AS enable_oauth_service_provider
           , MAX(COALESCE(r.enable_only_admin_integrations, s.enable_only_admin_integrations))                          AS enable_only_admin_integrations
           , MAX(COALESCE(r.enable_outgoing_webhooks, s.enable_outgoing_webhooks))                                AS enable_outgoing_webhooks
           , MAX(COALESCE(r.enable_post_icon_override, s.enable_post_icon_override))                               AS enable_post_icon_override
           , MAX(COALESCE(r.enable_post_search, s.enable_post_search))                                      AS enable_post_search
           , MAX(COALESCE(r.enable_post_username_override, s.enable_post_username_override))                           AS enable_post_username_override
           , MAX(COALESCE(r.enable_preview_features, s.enable_preview_features))                                 AS enable_preview_features
           , MAX(COALESCE(r.enable_security_fix_alert, s.enable_security_fix_alert))                               AS enable_security_fix_alert
           , MAX(COALESCE(r.enable_svgs, s.enable_svgs))                                             AS enable_svgs
           , MAX(COALESCE(r.enable_testing, s.enable_testing))                                          AS enable_testing
           , MAX(COALESCE(r.enable_tutorial, s.enable_tutorial))                                         AS enable_tutorial
           , MAX(COALESCE(r.enable_user_access_tokens, s.enable_user_access_tokens))                               AS enable_user_access_tokens
           , MAX(COALESCE(r.enable_user_statuses, s.enable_user_statuses))                                    AS enable_user_statuses
           , MAX(COALESCE(r.enable_user_typing_messages, s.enable_user_typing_messages))                             AS enable_user_typing_messages
           , MAX(COALESCE(r.enforce_multifactor_authentication, s.enforce_multifactor_authentication))                      AS enforce_multifactor_authentication
           , MAX(COALESCE(r.experimental_channel_organization, s.experimental_channel_organization))                       AS experimental_channel_organization
           , MAX(COALESCE(r.experimental_enable_authentication_transfer, s.experimental_enable_authentication_transfer))             AS experimental_enable_authentication_transfer
           , MAX(COALESCE(r.experimental_enable_default_channel_leave_join_messages, s.experimental_enable_default_channel_leave_join_messages)) AS experimental_enable_default_channel_leave_join_messages
           , MAX(COALESCE(r.experimental_enable_hardened_mode, s.experimental_enable_hardened_mode))                       AS experimental_enable_hardened_mode
           , MAX(COALESCE(r.experimental_group_unread_channels, s.experimental_group_unread_channels))                      AS experimental_group_unread_channels
           , MAX(COALESCE(s.experimental_ldap_group_sync, NULL ))                            AS experimental_ldap_group_sync
           , MAX(COALESCE(s.experimental_limit_client_config, NULL ))                        AS experimental_limit_client_config
           , MAX(COALESCE(r.experimental_strict_csrf_enforcement, s.experimental_strict_csrf_enforcement))                    AS experimental_strict_csrf_enforcement
           , MAX(COALESCE(r.forward_80_to_443, s.forward_80_to_443))                                       AS forward_80_to_443
           , MAX(COALESCE(r.gfycat_api_key, s.gfycat_api_key))                                          AS gfycat_api_key
           , MAX(COALESCE(r.gfycat_api_secret, s.gfycat_api_secret))                                       AS gfycat_api_secret
           , MAX(COALESCE(r.isdefault_allowed_untrusted_internal_connections, s.isdefault_allowed_untrusted_internal_connections))        AS isdefault_allowed_untrusted_internal_connections
           , MAX(COALESCE(s.isdefault_allowed_untrusted_inteznal_connections, NULL))        AS isdefault_allowed_untrusted_inteznal_connections
           , MAX(COALESCE(r.isdefault_allow_cors_from, s.isdefault_allow_cors_from))                               AS isdefault_allow_cors_from
           , MAX(COALESCE(r.isdefault_cors_exposed_headers, s.isdefault_cors_exposed_headers))                          AS isdefault_cors_exposed_headers
           , MAX(COALESCE(r.isdefault_google_developer_key, s.isdefault_google_developer_key))                          AS isdefault_google_developer_key
           , MAX(COALESCE(s.isdefault_image_proxy_options, NULL))                           AS isdefault_image_proxy_options
           , MAX(COALESCE(s.isdefault_image_proxy_type, NULL))                              AS isdefault_image_proxy_type
           , MAX(COALESCE(s.isdefault_image_proxy_url, NULL ))                               AS isdefault_image_proxy_url
           , MAX(COALESCE(r.isdefault_read_timeout, s.isdefault_read_timeout))                                  AS isdefault_read_timeout
           , MAX(COALESCE(r.isdefault_site_url, s.isdefault_site_url))                                      AS isdefault_site_url
           , MAX(COALESCE(r.isdefault_tls_cert_file, s.isdefault_tls_cert_file))                                 AS isdefault_tls_cert_file
           , MAX(COALESCE(r.isdefault_tls_key_file, s.isdefault_tls_key_file))                                  AS isdefault_tls_key_file
           , MAX(COALESCE(r.isdefault_write_timeout, s.isdefault_write_timeout))                                 AS isdefault_write_timeout
           , MAX(COALESCE(r.maximum_login_attempts, s.maximum_login_attempts))                                  AS maximum_login_attempts
           , MAX(COALESCE(r.minimum_hashtag_length, s.minimum_hashtag_length))                                  AS minimum_hashtag_length
           , MAX(COALESCE(r.post_edit_time_limit, s.post_edit_time_limit))                                    AS post_edit_time_limit
           , MAX(COALESCE(r.restrict_custom_emoji_creation, s.restrict_custom_emoji_creation))                          AS restrict_custom_emoji_creation
           , MAX(COALESCE(r.restrict_post_delete, s.restrict_post_delete))                                    AS restrict_post_delete
           , MAX(COALESCE(r.session_cache_in_minutes, s.session_cache_in_minutes))                                AS session_cache_in_minutes
           , MAX(COALESCE(r.session_idle_timeout_in_minutes, s.session_idle_timeout_in_minutes))                         AS session_idle_timeout_in_minutes
           , MAX(COALESCE(r.session_length_mobile_in_days, s.session_length_mobile_in_days))                           AS session_length_mobile_in_days
           , MAX(COALESCE(r.session_length_sso_in_days, s.session_length_sso_in_days))                              AS session_length_sso_in_days
           , MAX(COALESCE(r.session_length_web_in_days, s.session_length_web_in_days))                              AS session_length_web_in_days
           , MAX(COALESCE(r.time_between_user_typing_updates_milliseconds, s.time_between_user_typing_updates_milliseconds))           AS time_between_user_typing_updates_milliseconds
           , MAX(COALESCE(r.tls_strict_transport, s.tls_strict_transport))                                    AS tls_strict_transport
           , MAX(COALESCE(r.uses_letsencrypt, s.uses_letsencrypt))                                        AS uses_letsencrypt
           , MAX(COALESCE(r.websocket_url, s.websocket_url))                                           AS websocket_url
           , MAX(COALESCE(r.web_server_mode, s.web_server_mode))                                         AS web_server_mode           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           , MAX(COALESCE(r.experimental_channel_sidebar_organization, s.experimental_channel_sidebar_organization)) AS experimental_channel_sidebar_organization
           , MAX(COALESCE(r.experimental_data_prefetch, NULL)) AS experimental_data_prefetch
           , MAX(COALESCE(r.extend_session_length_with_activity, s.extend_session_length_with_activity)) AS extend_session_length_with_activity
           , MAX(COALESCE(r.enable_api_channel_deletion, NULL))        AS enable_api_channel_deletion
           , MAX(COALESCE(r.enable_api_user_deletion, NULL))        AS enable_api_user_deletion
           , MAX(COALESCE(r.enable_link_previews, NULL)) AS enable_link_previews
           , MAX(COALESCE(r.RESTRICT_LINK_PREVIEWS, NULL)) AS restrict_link_previews
           , MAX(COALESCE(r.enable_file_search, NULL)) AS enable_file_search
           , MAX(COALESCE(r.THREAD_AUTO_FOLLOW, NULL)) AS thread_autofollow
           , MAX(COALESCE(r.enable_legacy_sidebar, NULL))    AS enable_legacy_sidebar
           , MAX(COALESCE(r.collapsed_threads, NULL)) AS collapsed_threads
           , MAX(COALESCE(r.managed_resource_paths, NULL)) AS managed_resource_paths
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_service') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_service') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 81, 82
     )
SELECT *
FROM server_service_details