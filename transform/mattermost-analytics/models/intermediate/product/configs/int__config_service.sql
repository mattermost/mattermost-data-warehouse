{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_xs"
    })
}}

select event_id
    , event_table
    , event_name
    , server_id
    , received_at
    , timestamp

    , null as installation_id
    , null as anonymous_id
    , null as server_ip

    , allow_cookies_for_subdomains
    , allow_edit_post_service
    , null as allow_persistent_notifications
    , null as allow_persistent_notifications_for_guests
    , null as allow_synced_drafts
    , close_unused_direct_messages
    , cluster_log_timeout_milliseconds
    , null as collapsed_threads
    , connection_security_service
    , cors_allow_credentials
    , cors_debug
    , null as custom_cert_header
    , custom_service_terms_enabled
    , null as default_team_name
    , null as developer_flags
    , disable_bots_when_owner_is_deactivated
    , disable_legacy_mfa
    , null as enable_api_channel_deletion
    , enable_apiv3
    , null as enable_api_post_deletion
    , enable_api_team_deletion
    , null as enable_api_trigger_admin_notification
    , null as enable_api_user_deletion
    , enable_bot_account_creation
    , enable_channel_viewed_messages_service
    , enable_commands_service
    , enable_custom_emoji_service
    , enable_developer_service
    , enable_email_invitations
    , enable_emoji_picker_service
    , null as enable_file_search
    , enable_gif_picker
    , enable_incoming_webhooks_service
    , enable_insecure_outgoing_connections_service
    , enable_latex
    , null as enable_legacy_sidebar
    , null as enable_link_previews
    , enable_local_mode
    , enable_multifactor_authentication_service
    , enable_oauth_service_provider_service
    , null as enable_onboarding_flow
    , enable_only_admin_integrations_service
    , enable_opentracing
    , null as enable_outgoing_oauth_connections
    , enable_outgoing_webhooks
    , null as enable_permalink_previews
    , enable_post_icon_override
    , enable_post_search
    , enable_post_username_override
    , enable_preview_features
    , enable_security_fix_alert
    , enable_svgs
    , enable_testing
    , enable_tutorial
    , enable_user_access_tokens
    , enable_user_statuses
    , enable_user_typing_messages
    , enforce_multifactor_authentication_service
    , experimental_channel_organization
    , experimental_channel_sidebar_organization
    , null as experimental_data_prefetch
    , experimental_enable_authentication_transfer
    , experimental_enable_default_channel_leave_join_messages
    , experimental_enable_hardened_mode
    , experimental_group_unread_channels
    , experimental_ldap_group_sync
    , experimental_limit_client_config
    , experimental_strict_csrf_enforcement
    , extend_session_length_with_activity
    , forward_80_to_443
    , gfycat_api_key
    , gfycat_api_secret
    , isdefault_allowed_untrusted_internal_connections
    , isdefault_allow_cors_from
    , isdefault_cors_exposed_headers
    , isdefault_google_developer_key
    , isdefault_idle_timeout
    , isdefault_image_proxy_options
    , isdefault_image_proxy_type
    , isdefault_image_proxy_url
    , isdefault_read_timeout
    , isdefault_site_url
    , isdefault_tls_cert_file
    , isdefault_tls_key_file
    , isdefault_write_timeout
    , null as limit_load_search_result
    , null as login_with_certificate
    , null as managed_resource_paths
    , maximum_login_attempts
    , null as maximum_payload_size
    , null as maximum_url_length
    , minimum_hashtag_length
    , null as outgoing_integrations_requests_timeout
    , null as persistent_notification_interval_minutes
    , null as persistent_notification_max_count
    , null as persistent_notification_max_recipients
    , post_edit_time_limit
    , null as post_priority
    , null as refresh_post_stats_run_time
    , restrict_custom_emoji_creation
    , null as restrict_link_previews
    , restrict_post_delete
    , null as self_hosted_expansion
    , null as self_hosted_purchase
    , session_cache_in_minutes
    , session_idle_timeout_in_minutes
    , session_length_mobile_in_days
    , null as session_length_mobile_in_hours
    , session_length_sso_in_days
    , null as session_length_sso_in_hours
    , session_length_web_in_days
    , null as session_length_web_in_hours
    , null as skip_login_page
    , null as terminate_sessions_on_password_change
    , null as thread_auto_follow
    , time_between_user_typing_updates_milliseconds
    , tls_strict_transport
    , uses_letsencrypt
    , websocket_url
    , web_server_mode

    , context_library_name
    , context_library_version
    , sent_at
    , original_timestamp
from {{ ref('int_mattermost2__config_service') }}
union
select event_id
    , event_table
    , event_name
    , server_id
    , received_at
    , timestamp

    , installation_id
    , anonymous_id
    , server_ip

    , allow_cookies_for_subdomains
    , allow_edit_post_service 
    , allow_persistent_notifications
    , allow_persistent_notifications_for_guests
    , allow_synced_drafts
    , close_unused_direct_messages
    , cluster_log_timeout_milliseconds
    , collapsed_threads
    , connection_security_service
    , cors_allow_credentials
    , cors_debug
    , custom_cert_header
    , null as custom_services_terms_enabled
    , default_team_name
    , developer_flags
    , disable_bots_when_owner_is_deactivated
    , disable_legacy_mfa
    , enable_api_channel_deletion
    , null as enable_apiv3
    , enable_api_post_deletion
    , enable_api_team_deletion
    , enable_api_trigger_admin_notification
    , enable_api_user_deletion
    , enable_bot_account_creation
    , enable_channel_viewed_messages_service
    , enable_commands_service
    , enable_custom_emoji_service
    , enable_developer_service
    , enable_email_invitations
    , enable_emoji_picker_service
    , enable_file_search
    , enable_gif_picker
    , enable_incoming_webhooks_service
    , enable_insecure_outgoing_connections_service
    , enable_latex
    , enable_legacy_sidebar
    , enable_link_previews
    , enable_local_mode
    , enable_multifactor_authentication_service
    , enable_oauth_service_provider_service
    , enable_onboarding_flow
    , enable_only_admin_integrations_service
    , enable_opentracing
    , enable_outgoing_oauth_connections
    , enable_outgoing_webhooks
    , enable_permalink_previews
    , enable_post_icon_override
    , enable_post_search
    , enable_post_username_override
    , enable_preview_features
    , enable_security_fix_alert
    , enable_svgs
    , enable_testing
    , enable_tutorial
    , enable_user_access_tokens
    , enable_user_statuses
    , enable_user_typing_messages
    , enforce_multifactor_authentication_service
    , experimental_channel_organization
    , experimental_channel_sidebar_organization
    , experimental_data_prefetch
    , experimental_enable_authentication_transfer
    , experimental_enable_default_channel_leave_join_messages
    , experimental_enable_hardened_mode
    , experimental_group_unread_channels
    , null as experimental_ldap_group_sync
    , null as experimental_limit_client_config
    , experimental_strict_csrf_enforcement
    , extend_session_length_with_activity
    , forward_80_to_443
    , gfycat_api_key
    , gfycat_api_secret
    , isdefault_allowed_untrusted_internal_connections
    , isdefault_allow_cors_from
    , isdefault_cors_exposed_headers
    , isdefault_google_developer_key
    , isdefault_idle_timeout
    , null as isdefault_image_proxy_options
    , null as isdefault_image_proxy_type
    , null as isdefault_image_proxy_url
    , isdefault_read_timeout
    , isdefault_site_url
    , isdefault_tls_cert_file
    , isdefault_tls_key_file
    , isdefault_write_timeout
    , limit_load_search_result
    , login_with_certificate
    , managed_resource_paths
    , maximum_login_attempts
    , maximum_payload_size
    , maximum_url_length
    , minimum_hashtag_length
    , outgoing_integrations_requests_timeout
    , persistent_notification_interval_minutes
    , persistent_notification_max_count
    , persistent_notification_max_recipients
    , post_edit_time_limit
    , post_priority
    , refresh_post_stats_run_time
    , restrict_custom_emoji_creation
    , restrict_link_previews
    , restrict_post_delete
    , self_hosted_expansion
    , self_hosted_purchase
    , session_cache_in_minutes
    , session_idle_timeout_in_minutes
    , session_length_mobile_in_days
    , session_length_mobile_in_hours
    , session_length_sso_in_days
    , session_length_sso_in_hours
    , session_length_web_in_days
    , session_length_web_in_hours
    , skip_login_page
    , terminate_sessions_on_password_change
    , thread_auto_follow
    , time_between_user_typing_updates_milliseconds
    , tls_strict_transport
    , uses_letsencrypt
    , websocket_url
    , web_server_mode

    , context_library_name
    , context_library_version
    , sent_at
    , original_timestamp
from {{ ref('int_mm_telemetry_prod__config_service') }}

