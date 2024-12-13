
with source as (
    select * from {{ source('mm_telemetry_prod', 'configs') }}
),

renamed as (
    select
        -- Common event columns
        id               as event_id
        , event          as event_table
        , event_text     as event_name
        , user_id        as server_id
        , received_at    as received_at
        , timestamp      as timestamp

        -- Server info
        , anonymous_id
        , context_ip as server_ip

         -- OAuth information
        , config_oauth_enable_gitlab                          as is_gitlab_enabled
        , config_oauth_enable_google                          as is_google_enabled
        , config_oauth_enable_office_365                      as is_office365_enabled
        , config_oauth_enable_openid                          as is_openid_enabled
        , config_oauth_openid_gitlab                          as is_openid_gitlab_enabled
        , config_oauth_openid_google                          as is_openid_google_enabled
        , config_oauth_openid_office_365                      as is_openid_office365_enabled

        -- Ldap information
        , config_ldap_connection_security                     as connection_security_ldap
        , config_ldap_enable                                  as enable_ldap
        , config_ldap_enable_admin_filter                     as enable_admin_filter
        , config_ldap_enable_sync                             as enable_sync
        , config_ldap_isdefault_email_attribute               as isdefault_email_attribute_ldap
        , config_ldap_isdefault_first_name_attribute          as isdefault_first_name_attribute_ldap
        , config_ldap_isdefault_group_display_name_attribute  as isdefault_group_display_name_attribute
        , config_ldap_isdefault_group_id_attribute            as isdefault_group_id_attribute
        , config_ldap_isdefault_id_attribute                  as isdefault_id_attribute_ldap
        , config_ldap_isdefault_last_name_attribute           as isdefault_last_name_attribute_ldap
        , config_ldap_isdefault_login_button_border_color     as isdefault_login_button_border_color_ldap
        , config_ldap_isdefault_login_button_color            as isdefault_login_button_color_ldap
        , config_ldap_isdefault_login_button_text_color       as isdefault_login_button_text_color_ldap
        , config_ldap_isdefault_login_field_name              as isdefault_login_field_name
        , config_ldap_isdefault_login_id_attribute            as isdefault_login_id_attribute
        , config_ldap_isdefault_nickname_attribute            as isdefault_nickname_attribute_ldap
        , config_ldap_isdefault_position_attribute            as isdefault_position_attribute_ldap
        , config_ldap_isdefault_username_attribute            as isdefault_username_attribute_ldap
        , config_ldap_isempty_admin_filter                    as isempty_admin_filter
        , config_ldap_isempty_group_filter                    as isempty_group_filter
        , config_ldap_isempty_guest_filter                    as isempty_guest_filter
        , config_ldap_isnotempty_picture_attribute            as isnotempty_picture_attribute
        , config_ldap_isnotempty_private_key                  as isnotempty_private_key
        , config_ldap_isnotempty_public_certificate           as isnotempty_public_certificate
        , config_ldap_max_page_size                           as max_page_size
        , config_ldap_query_timeout                           as query_timeout_ldap
        , config_ldap_skip_certificate_verification           as skip_certificate_verification
        , config_ldap_sync_interval_minutes                   as sync_interval_minutes

        -- Saml information
        , config_saml_enable                                  as enable_saml 
        , config_saml_enable_admin_attribute                  as enable_admin_attribute
        , config_saml_enable_sync_with_ldap                   as enable_sync_with_ldap
        , config_saml_enable_sync_with_ldap_include_auth      as enable_sync_with_ldap_include_auth
        , config_saml_encrypt                                 as encrypt_saml
        , config_saml_ignore_guests_ldap_sync                 as ignore_guests_ldap_sync
        , config_saml_isdefault_admin_attribute               as isdefault_admin_attribute
        , config_saml_isdefault_canonical_algorithm           as isdefault_canonical_algorithm
        , config_saml_isdefault_email_attribute               as isdefault_email_attribute_saml
        , config_saml_isdefault_first_name_attribute          as isdefault_first_name_attribute_saml
        , config_saml_isdefault_guest_attribute               as isdefault_guest_attribute
        , config_saml_isdefault_id_attribute                  as isdefault_id_attribute_saml
        , config_saml_isdefault_last_name_attribute           as isdefault_last_name_attribute_saml
        , config_saml_isdefault_locale_attribute              as isdefault_locale_attribute
        , config_saml_isdefault_login_button_border_color     as isdefault_login_button_border_color_saml
        , config_saml_isdefault_login_button_color            as isdefault_login_button_color_saml
        , config_saml_isdefault_login_button_text             as isdefault_login_button_text
        , config_saml_isdefault_login_button_text_color       as isdefault_login_button_text_color_saml
        , config_saml_isdefault_nickname_attribute            as isdefault_nickname_attribute_saml
        , config_saml_isdefault_position_attribute            as isdefault_position_attribute_saml
        , config_saml_isdefault_scoping_idp_name              as isdefault_scoping_idp_name
        , config_saml_isdefault_scoping_idp_provider_id       as isdefault_scoping_idp_provider_id
        , config_saml_isdefault_signature_algorithm           as isdefault_signature_algorithm
        , config_saml_isdefault_username_attribute            as isdefault_username_attribute_saml
        , config_saml_sign_request                            as sign_request
        , config_saml_verify                                  as verify_saml
        
        -- Plugin information
        , config_plugin_allow_insecure_download_url                                               as allow_insecure_download_url
        , config_plugin_automatic_prepackaged_plugins                                             as automatic_prepackaged_plugins
        , config_plugin_enable                                                                    as enable_plugin
        , config_plugin_enable_antivirus                                                          as enable_antivirus
        , config_plugin_enable_mattermost_autolink                                                as enable_mattermost_autolink
        , config_plugin_enable_com_mattermost_aws_sns                                             as enable_aws_sns
        , config_plugin_enable_com_mattermost_plugin_channel_export                               as enable_channel_export
        , config_plugin_enable_com_mattermost_confluence                                          as enable_confluence
        , config_plugin_enable_com_mattermost_custom_attributes                                   as enable_custom_user_attributes
        , config_plugin_enable_com_github_phillipahereza_mattermost_plugin_digitalocean           as enable_digitalocean
        , config_plugin_enable_focalboard                                                         as enable_focalboard
        , config_plugin_enable_com_github_moussetc_mattermost_plugin_giphy                        as enable_giphy
        , config_plugin_enable_github                                                             as enable_github
        , config_plugin_enable_com_github_manland_mattermost_plugin_gitlab                        as enable_gitlab
        , config_plugin_enable_health_check                                                       as enable_health_check
        , config_plugin_enable_com_mattermost_plugin_incident_management                          as enable_incident_management
        , config_plugin_enable_jenkins                                                            as enable_jenkins
        , config_plugin_enable_jira                                                               as enable_jira
        , config_plugin_enable_jitsi                                                              as enable_jitsi
        , config_plugin_enable_marketplace                                                        as enable_marketplace
        , config_plugin_enable_com_mattermost_calls                                               as enable_mattermost_calls
        , config_plugin_enable_com_mattermost_msteams_sync                                        as enable_mattermost_msteams_sync
        , config_plugin_enable_mattermost_plugin_servicenow                                       as enable_mattermost_servicenow
        , config_plugin_enable_memes                                                              as enable_memes
        , config_plugin_enable_com_mattermost_mscalendar                                          as enable_mscalendar
        , config_plugin_enable_com_mattermost_nps                                                 as enable_nps
        , config_plugin_enable_nps_survey                                                         as enable_nps_survey
        , config_plugin_enable_playbooks                                                          as enable_playbooks
        , config_plugin_enable_remote_marketplace                                                 as enable_remote_marketplace
        , config_plugin_enable_skype_4_business                                                   as enable_skype4business
        , config_plugin_enable_com_mattermost_plugin_todo                                         as enable_todo
        , config_plugin_enable_uploads                                                            as enable_uploads
        , config_plugin_enable_com_mattermost_webex                                               as enable_webex
        , config_plugin_enable_com_mattermost_welcomebot                                          as enable_welcome_bot
        , config_plugin_enable_zoom                                                               as enable_zoom
        , config_plugin_is_default_marketplace_url                                                as is_default_marketplace_url
        , config_plugin_require_plugin_signature                                                  as require_plugin_signature
        , config_plugin_signature_public_key_files                                                as signature_public_key_files
        , config_plugin_version_mattermost_autolink                                               as version_autolink
        , config_plugin_version_com_mattermost_aws_sns                                            as version_aws_sns
        , config_plugin_version_com_mattermost_confluence                                         as version_confluence
        , config_plugin_version_com_mattermost_custom_attributes                                  as version_custom_user_attributes
        , config_plugin_version_focalboard                                                        as version_focalboard
        , config_plugin_version_com_github_moussetc_mattermost_plugin_giphy                       as version_giphy
        , config_plugin_version_github                                                            as version_github
        , config_plugin_version_com_github_manland_mattermost_plugin_gitlab                       as version_gitlab
        , config_plugin_version_jira                                                              as version_jira
        , config_plugin_version_com_mattermost_calls                                              as version_mattermost_calls
        , config_plugin_version_com_mattermost_msteams_sync                                       as version_mattermost_msteams_sync
        , config_plugin_version_memes                                                             as version_memes
        , config_plugin_version_com_mattermost_nps                                                as version_nps
        , config_plugin_version_playbooks                                                         as version_playbooks
        , config_plugin_version_com_mattermost_plugin_todo                                        as version_todo
        , config_plugin_version_com_mattermost_welcomebot                                         as version_welcome_bot
        , config_plugin_version_zoom                                                              as version_zoom
        -- Service information
        , config_service_allow_cookies_for_subdomains                                             as allow_cookies_for_subdomains
        , config_service_allow_persistent_notifications                                           as allow_persistent_notifications
        , config_service_allow_persistent_notifications_for_guests                                as allow_persistent_notifications_for_guests
        , config_service_allow_synced_drafts                                                      as allow_synced_drafts
        , config_service_cluster_log_timeout_milliseconds                                         as cluster_log_timeout_milliseconds
        , config_service_collapsed_threads                                                        as collapsed_threads
        , config_service_connection_security                                                      as connection_security_service
        , config_service_cors_allow_credentials                                                   as cors_allow_credentials
        , config_service_cors_debug                                                               as cors_debug
        , config_service_developer_flags                                                          as developer_flags
        , config_service_disable_bots_when_owner_is_deactivated                                   as disable_bots_when_owner_is_deactivated
        , config_service_enable_api_channel_deletion                                              as enable_api_channel_deletion
        , config_service_enable_api_post_deletion                                                 as enable_api_post_deletion
        , config_service_enable_api_team_deletion                                                 as enable_api_team_deletion
        , config_service_enable_api_trigger_admin_notification                                    as enable_api_trigger_admin_notification
        , config_service_enable_api_user_deletion                                                 as enable_api_user_deletion
        , config_service_enable_bot_account_creation                                              as enable_bot_account_creation
        , config_service_enable_channel_viewed_messages                                           as enable_channel_viewed_messages_service
        , config_service_enable_client_performance_debugging                                      as enable_client_performance_debugging_service
        , config_service_enable_commands                                                          as enable_commands_service
        , config_service_enable_custom_emoji                                                      as enable_custom_emoji_service
        , config_service_enable_custom_groups                                                     as enable_custom_groups_service
        , config_service_enable_developer                                                         as enable_developer_service
        , config_service_enable_email_invitations                                                 as enable_email_invitations
        , config_service_enable_emoji_picker                                                      as enable_emoji_picker_service
        , config_service_enable_file_search                                                       as enable_file_search
        , config_service_enable_gif_picker                                                        as enable_gif_picker
        , config_service_enable_incoming_webhooks                                                 as enable_incoming_webhooks_service
        , config_service_enable_inline_latex                                                      as enable_inline_latex
        , config_service_enable_insecure_outgoing_connections                                     as enable_insecure_outgoing_connections_service
        , config_service_enable_latex                                                             as enable_latex
        , config_service_enable_link_previews                                                     as enable_link_previews
        , config_service_enable_local_mode                                                        as enable_local_mode
        , config_service_enable_multifactor_authentication                                        as enable_multifactor_authentication_service
        , config_service_enable_oauth_service_provider                                            as enable_oauth_service_provider_service
        , config_service_enable_onboarding_flow                                                   as enable_onboarding_flow
        , config_service_enable_opentracing                                                       as enable_opentracing
        , config_service_enable_outgoing_oauth_connections                                        as enable_outgoing_oauth_connections
        , config_service_enable_outgoing_webhooks                                                 as enable_outgoing_webhooks
        , config_service_enable_permalink_previews                                                as enable_permalink_previews
        , config_service_enable_post_icon_override                                                as enable_post_icon_override
        , config_service_enable_post_search                                                       as enable_post_search
        , config_service_enable_post_username_override                                            as enable_post_username_override
        , config_service_enable_security_fix_alert                                                as enable_security_fix_alert
        , config_service_enable_svgs                                                              as enable_svgs
        , config_service_enable_testing                                                           as enable_testing
        , config_service_enable_tutorial                                                          as enable_tutorial
        , config_service_enable_user_access_tokens                                                as enable_user_access_tokens
        , config_service_enable_user_statuses                                                     as enable_user_statuses
        , config_service_enable_user_typing_messages                                              as enable_user_typing_messages
        , config_service_enforce_multifactor_authentication                                       as enforce_multifactor_authentication_service
        , config_service_experimental_enable_authentication_transfer                              as experimental_enable_authentication_transfer
        , config_service_experimental_enable_default_channel_leave_join_messages                  as experimental_enable_default_channel_leave_join_messages
        , config_service_experimental_enable_hardened_mode                                        as experimental_enable_hardened_mode
        , config_service_experimental_group_unread_channels                                       as experimental_group_unread_channels
        , config_service_experimental_strict_csrf_enforcement                                     as experimental_strict_csrf_enforcement
        , config_service_extend_session_length_with_activity                                      as extend_session_length_with_activity
        , config_service_forward_80_to_443                                                        as forward_80_to_443
        , config_service_isdefault_allowed_untrusted_internal_connections                         as isdefault_allowed_untrusted_internal_connections
        , config_service_isdefault_allow_cors_from                                                as isdefault_allow_cors_from
        , config_service_isdefault_cors_exposed_headers                                           as isdefault_cors_exposed_headers
        , config_service_isdefault_google_developer_key                                           as isdefault_google_developer_key
        , config_service_isdefault_idle_timeout                                                   as isdefault_idle_timeout
        , config_service_isdefault_read_timeout                                                   as isdefault_read_timeout
        , config_service_isdefault_site_url                                                       as isdefault_site_url
        , config_service_isdefault_tls_cert_file                                                  as isdefault_tls_cert_file
        , config_service_isdefault_tls_key_file                                                   as isdefault_tls_key_file
        , config_service_isdefault_write_timeout                                                  as isdefault_write_timeout
        , config_service_managed_resource_paths                                                   as managed_resource_paths
        , config_service_maximum_login_attempts                                                   as maximum_login_attempts
        , config_service_maximum_payload_size                                                     as maximum_payload_size
        , config_service_maximum_url_length                                                       as maximum_url_length
        , config_service_minimum_hashtag_length                                                   as minimum_hashtag_length
        , config_service_outgoing_integrations_requests_timeout                                   as outgoing_integrations_requests_timeout
        , config_service_persistent_notification_interval_minutes                                 as persistent_notification_interval_minutes
        , config_service_persistent_notification_max_count                                        as persistent_notification_max_count
        , config_service_persistent_notification_max_recipients                                   as persistent_notification_max_recipients
        , config_service_post_edit_time_limit                                                     as post_edit_time_limit
        , config_service_post_priority                                                            as post_priority
        , config_service_refresh_post_stats_run_time                                              as refresh_post_stats_run_time
        , config_service_restrict_link_previews                                                   as restrict_link_previews
        , config_service_session_cache_in_minutes                                                 as session_cache_in_minutes
        , config_service_session_idle_timeout_in_minutes                                          as session_idle_timeout_in_minutes
        , config_service_session_length_mobile_in_hours                                           as session_length_mobile_in_hours
        , config_service_session_length_sso_in_hours                                              as session_length_sso_in_hours
        , config_service_session_length_web_in_hours                                              as session_length_web_in_hours
        , config_service_terminate_sessions_on_password_change                                    as terminate_sessions_on_password_change
        , config_service_thread_auto_follow                                                       as thread_auto_follow
        , config_service_time_between_user_typing_updates_milliseconds                            as time_between_user_typing_updates_milliseconds
        , config_service_tls_strict_transport                                                     as tls_strict_transport
        , config_service_uses_letsencrypt                                                         as uses_letsencrypt
        , config_service_websocket_url                                                            as websocket_url
        , config_service_web_server_mode                                                          as web_server_mode
        
       -- Ignored - Always null
        -- , channel
        -- Metadata from Rudderstack
        , context_library_name
        , context_library_version
        , sent_at
        , original_timestamp

        -- Ignored -- Always same value
        -- , context_destination_id
        -- , context_destination_type
        -- , context_source_id
        -- , context_source_type
        -- Ignored - used by Rudderstack for debugging purposes
        -- , uuid_ts
    from source
)

select * from renamed
