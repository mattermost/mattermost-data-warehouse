with source as (
    select * from {{ source('mattermost2', 'config_service') }}
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

        , allow_cookies_for_subdomains
        , allow_edit_post                      as allow_edit_post_service
        , close_unused_direct_messages
        , cluster_log_timeout_milliseconds
        , connection_security                  as connection_security_service
        , cors_allow_credentials
        , cors_debug
        , custom_service_terms_enabled
        , disable_bots_when_owner_is_deactivated
        , disable_legacy_mfa
        , enable_apiv3
        , enable_api_team_deletion
        , enable_bot_account_creation
        , enable_channel_viewed_messages       as enable_channel_viewed_messages_service
        , enable_commands                      as enable_commands_service
        , enable_custom_emoji                  as enable_custom_emoji_service
        , enable_developer                     as enable_developer_service
        , enable_email_invitations
        , enable_emoji_picker                  as enable_emoji_picker_service
        , enable_gif_picker
        , enable_incoming_webhooks             as enable_incoming_webhooks_service
        , enable_insecure_outgoing_connections as enable_insecure_outgoing_connections_service
        , enable_latex
        , enable_local_mode
        , enable_multifactor_authentication    as enable_multifactor_authentication_service
        , enable_oauth_service_provider        as enable_oauth_service_provider_service
        , enable_only_admin_integrations       as enable_only_admin_integrations_service
        , enable_opentracing
        , enable_outgoing_webhooks
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
        , enforce_multifactor_authentication   as enforce_multifactor_authentication_service
        , experimental_channel_organization
        , experimental_channel_sidebar_organization
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
        , coalesce (isdefault_allowed_untrusted_internal_connections, 
                    isdefault_allowed_untrusted_inteznal_connections) as isdefault_allowed_untrusted_internal_connections
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
        , maximum_login_attempts
        , minimum_hashtag_length
        , post_edit_time_limit
        , restrict_custom_emoji_creation
        , restrict_post_delete
        , session_cache_in_minutes
        , session_idle_timeout_in_minutes
        , session_length_mobile_in_days
        , session_length_sso_in_days
        , session_length_web_in_days
        , time_between_user_typing_updates_milliseconds
        , tls_strict_transport
        , uses_letsencrypt
        , websocket_url
        , web_server_mode

        -- Metadata from Segment
        , context_library_name
        , context_library_version
        , sent_at
        , try_to_timestamp_ntz(original_timestamp) as original_timestamp

        -- Ignored - used by segment for debugging purposes
        -- , uuid_ts

    from source
)

select * from renamed
