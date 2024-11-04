{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

with int_config_oauth as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.star(ref('int__config_oauth')) }}
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
    from
        {{ ref('int__config_oauth') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), int_config_ldap as (
    select
        cast(timestamp as date) as server_date
        , {{ dbt_utils.star(ref('int__config_ldap')) }}
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
    from
        {{ ref('int__config_ldap') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), int_config_saml as (
    select
        cast(timestamp as date) as server_date
        , {{ dbt_utils.star(ref('int__config_saml')) }}
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
    from
        {{ ref('int__config_saml') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), int_config_plugin as (
    select
        cast(timestamp as date) as server_date
        , {{ dbt_utils.star(ref('int__config_plugin')) }}
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
    from
        {{ ref('int__config_plugin') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), int_config_service as (
    select
        cast(timestamp as date) as server_date
        , {{ dbt_utils.star(ref('int__config_service')) }}
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
    from
        {{ ref('int__config_service') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
)

select
    spine.daily_server_id
    , spine.server_id
    , spine.activity_date
    -- OAuth section
    , os.is_office365_enabled
    , os.is_google_enabled
    , os.is_gitlab_enabled
    , os.is_openid_enabled
    , os.is_openid_google_enabled
    , os.is_openid_gitlab_enabled
    , os.is_openid_office365_enabled
   
    -- Ldap section
    , ls.connection_security_ldap
    , ls.enable_ldap
    , ls.enable_admin_filter
    , ls.enable_sync
    , ls.isdefault_email_attribute_ldap
    , ls.isdefault_first_name_attribute_ldap
    , ls.isdefault_group_display_name_attribute
    , ls.isdefault_group_id_attribute
    , ls.isdefault_id_attribute_ldap
    , ls.isdefault_last_name_attribute_ldap
    , ls.isdefault_login_button_border_color_ldap
    , ls.isdefault_login_button_color_ldap
    , ls.isdefault_login_button_text_color_ldap
    , ls.isdefault_login_field_name
    , ls.isdefault_login_id_attribute
    , ls.isdefault_nickname_attribute_ldap
    , ls.isdefault_position_attribute_ldap
    , ls.isdefault_username_attribute_ldap
    , ls.isempty_admin_filter
    , ls.isempty_group_filter
    , ls.isempty_guest_filter
    , ls.isnotempty_picture_attribute
    , ls.isnotempty_private_key
    , ls.isnotempty_public_certificate
    , ls.max_page_size
    , ls.query_timeout_ldap
    , ls.segment_dedupe_id_ldap
    , ls.skip_certificate_verification
    , ls.sync_interval_minutes
    
    -- Saml section
    , ss.enable_saml 
    , ss.enable_admin_attribute
    , ss.enable_sync_with_ldap
    , ss.enable_sync_with_ldap_include_auth
    , ss.encrypt_saml
    , ss.ignore_guests_ldap_sync
    , ss.isdefault_admin_attribute
    , ss.isdefault_canonical_algorithm
    , ss.isdefault_email_attribute_saml
    , ss.isdefault_first_name_attribute_saml
    , ss.isdefault_guest_attribute
    , ss.isdefault_id_attribute_saml
    , ss.isdefault_last_name_attribute_saml
    , ss.isdefault_locale_attribute
    , ss.isdefault_login_button_border_color_saml
    , ss.isdefault_login_button_color_saml
    , ss.isdefault_login_button_text
    , ss.isdefault_login_button_text_color_saml
    , ss.isdefault_nickname_attribute_saml
    , ss.isdefault_position_attribute_saml
    , ss.isdefault_scoping_idp_name
    , ss.isdefault_scoping_idp_provider_id
    , ss.isdefault_signature_algorithm
    , ss.isdefault_username_attribute_saml
    , ss.sign_request
    , ss.verify_saml         
        
    -- Plugin section
    , ps.allow_insecure_download_url
    , ps.automatic_prepackaged_plugins
    , ps.chimera_oauth_proxy_url
    , ps.enable_plugin
    , ps.enable_alertmanager
    , ps.enable_antivirus
    , ps.enable_autolink
    , ps.enable_aws_sns
    , ps.enable_bitbucket
    , ps.enable_channel_export
    , ps.enable_circleci
    , ps.enable_confluence
    , ps.enable_custom_user_attributes
    , ps.enable_diceroller
    , ps.enable_digitalocean
    , ps.enable_focalboard
    , ps.enable_giphy
    , ps.enable_github
    , ps.enable_gitlab
    , ps.enable_health_check
    , ps.enable_icebreaker
    , ps.enable_incident_management
    , ps.enable_incident_response
    , ps.enable_jenkins
    , ps.enable_jespino_recommend
    , ps.enable_jira
    , ps.enable_jitsi
    , ps.enable_marketplace
    , ps.enable_matterpoll
    , ps.enable_mattermost_agenda
    , ps.enable_mattermost_apps
    , ps.enable_mattermost_azure_devops
    , ps.enable_mattermost_calls
    , ps.enable_mattermost_hackerone
    , ps.enable_mattermost_msteams_meetings
    , ps.enable_mattermost_msteams_sync
    , ps.enable_mattermost_profanity_filter
    , ps.enable_mattermost_servicenow
    , ps.enable_mattermost_servicenow_virtual_agent
    , ps.enable_memes
    , ps.enable_mscalendar
    , ps.enable_nps
    , ps.enable_nps_survey
    , ps.enable_playbooks
    , ps.enable_remote_marketplace
    , ps.enable_ru_loop_plugin_embeds
    , ps.enable_ru_loop_plugin_scheduler
    , ps.enable_ru_loop_plugin_user_fields
    , ps.enable_set_default_theme
    , ps.enable_skype4business
    , ps.enable_todo
    , ps.enable_uploads
    , ps.enable_webex
    , ps.enable_welcome_bot
    , ps.enable_zoom
    , ps.is_default_marketplace_url
    , ps.require_plugin_signature
    , ps.signature_public_key_files
    , ps.version_alertmanager
    , ps.version_antivirus
    , ps.version_autolink
    , ps.version_aws_sns
    , ps.version_bitbucket
    , ps.version_channel_export
    , ps.version_circleci
    , ps.version_confluence
    , ps.version_custom_user_attributes
    , ps.version_diceroller
    , ps.version_digitalocean
    , ps.version_giphy
    , ps.version_github
    , ps.version_gitlab
    , ps.version_icebreaker
    , ps.version_incident_management
    , ps.version_incident_response
    , ps.version_jenkins
    , ps.version_jespino_recommend
    , ps.version_jira
    , ps.version_jitsi
    , ps.version_matterpoll
    , ps.version_mattermost_agenda
    , ps.version_mattermost_apps
    , ps.version_mattermost_azure_devops
    , ps.version_mattermost_calls
    , ps.version_mattermost_hackerone
    , ps.version_mattermost_msteams_meetings
    , ps.version_mattermost_msteams_sync
    , ps.version_mattermost_profanity_filter
    , ps.version_mattermost_servicenow
    , ps.version_mattermost_servicenow_virtual_agent
    , ps.version_memes
    , ps.version_mscalendar
    , ps.version_nps
    , ps.version_playbooks
    , ps.version_set_default_theme
    , ps.version_todo
    , ps.version_webex
    , ps.version_welcome_bot
    , ps.version_zoom

    -- Service section
    , vs.allow_cookies_for_subdomains
    , vs.allow_edit_post_service
    , vs.allow_persistent_notifications
    , vs.allow_persistent_notifications_for_guests
    , vs.allow_synced_drafts
    , vs.close_unused_direct_messages
    , vs.cluster_log_timeout_milliseconds
    , vs.collapsed_threads
    , vs.connection_security_service
    , vs.cors_allow_credentials
    , vs.cors_debug
    , vs.custom_cert_header
    , vs.custom_service_terms_enabled
    , vs.default_team_name
    , vs.developer_flags
    , vs.disable_bots_when_owner_is_deactivated
    , vs.disable_legacy_mfa
    , vs.enable_api_channel_deletion
    , vs.enable_apiv3
    , vs.enable_api_post_deletion
    , vs.enable_api_team_deletion
    , vs.enable_api_trigger_admin_notification
    , vs.enable_api_user_deletion
    , vs.enable_bot_account_creation
    , vs.enable_channel_viewed_messages_service
    , vs.enable_commands_service
    , vs.enable_custom_emoji_service
    , vs.enable_developer_service
    , vs.enable_email_invitations
    , vs.enable_emoji_picker_service
    , vs.enable_file_search
    , vs.enable_gif_picker
    , vs.enable_incoming_webhooks_service
    , vs.enable_insecure_outgoing_connections_service
    , vs.enable_latex
    , vs.enable_legacy_sidebar
    , vs.enable_link_previews
    , vs.enable_local_mode
    , vs.enable_multifactor_authentication_service
    , vs.enable_oauth_service_provider_service
    , vs.enable_onboarding_flow
    , vs.enable_only_admin_integrations_service
    , vs.enable_opentracing
    , vs.enable_outgoing_oauth_connections
    , vs.enable_outgoing_webhooks
    , vs.enable_permalink_previews
    , vs.enable_post_icon_override
    , vs.enable_post_search
    , vs.enable_post_username_override
    , vs.enable_preview_features
    , vs.enable_security_fix_alert
    , vs.enable_svgs
    , vs.enable_testing
    , vs.enable_tutorial
    , vs.enable_user_access_tokens
    , vs.enable_user_statuses
    , vs.enable_user_typing_messages
    , vs.enforce_multifactor_authentication_service
    , vs.experimental_channel_organization
    , vs.experimental_channel_sidebar_organization
    , vs.experimental_data_prefetch
    , vs.experimental_enable_authentication_transfer
    , vs.experimental_enable_default_channel_leave_join_messages
    , vs.experimental_enable_hardened_mode
    , vs.experimental_group_unread_channels
    , vs.experimental_ldap_group_sync
    , vs.experimental_limit_client_config
    , vs.experimental_strict_csrf_enforcement
    , vs.extend_session_length_with_activity
    , vs.forward_80_to_443
    , vs.gfycat_api_key
    , vs.gfycat_api_secret
    , vs.isdefault_allowed_untrusted_internal_connections
    , vs.isdefault_allow_cors_from
    , vs.isdefault_cors_exposed_headers
    , vs.isdefault_google_developer_key
    , vs.isdefault_idle_timeout
    , vs.isdefault_image_proxy_options
    , vs.isdefault_image_proxy_type
    , vs.isdefault_image_proxy_url
    , vs.isdefault_read_timeout
    , vs.isdefault_site_url
    , vs.isdefault_tls_cert_file
    , vs.isdefault_tls_key_file
    , vs.isdefault_write_timeout
    , vs.limit_load_search_result
    , vs.login_with_certificate
    , vs.managed_resource_paths
    , vs.maximum_login_attempts
    , vs.maximum_payload_size
    , vs.maximum_url_length
    , vs.minimum_hashtag_length
    , vs.outgoing_integrations_requests_timeout
    , vs.persistent_notification_interval_minutes
    , vs.persistent_notification_max_count
    , vs.persistent_notification_max_recipients
    , vs.post_edit_time_limit
    , vs.post_priority
    , vs.refresh_post_stats_run_time
    , vs.restrict_custom_emoji_creation
    , vs.restrict_link_previews
    , vs.restrict_post_delete
    , vs.self_hosted_expansion
    , vs.self_hosted_purchase
    , vs.session_cache_in_minutes
    , vs.session_idle_timeout_in_minutes
    , vs.session_length_mobile_in_days
    , vs.session_length_mobile_in_hours
    , vs.session_length_sso_in_days
    , vs.session_length_sso_in_hours
    , vs.session_length_web_in_days
    , vs.session_length_web_in_hours
    , vs.skip_login_page
    , vs.terminate_sessions_on_password_change
    , vs.thread_auto_follow
    , vs.time_between_user_typing_updates_milliseconds
    , vs.tls_strict_transport
    , vs.uses_letsencrypt
    , vs.websocket_url
    , vs.web_server_mode      
    
    -- Metadata
    , os.has_segment_telemetry_data or ls.has_segment_telemetry_data or ss.has_segment_telemetry_data or ps.has_segment_telemetry_data or vs.has_segment_telemetry_data as has_segment_telemetry_data
    , os.has_rudderstack_telemetry_data or ls.has_rudderstack_telemetry_data or ss.has_rudderstack_telemetry_data or ps.has_rudderstack_telemetry_data or vs.has_rudderstack_telemetry_data as has_rudderstack_telemetry_data
from
    {{ ref('int_server_active_days_spined') }} spine
    left join int_config_oauth os on spine.daily_server_id = os.daily_server_id
    left join int_config_ldap ls on spine.daily_server_id = ls.daily_server_id
    left join int_config_saml ss on spine.daily_server_id = ss.daily_server_id
    left join int_config_plugin ps on spine.daily_server_id = ps.daily_server_id
    left join int_config_service vs on spine.daily_server_id = vs.daily_server_id
