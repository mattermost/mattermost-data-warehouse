{{
    config({
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
), int__config_all as (
    select
        cast(timestamp as date) as server_date
        , {{ dbt_utils.star(ref('int__config_all')) }}
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
    from
        {{ ref('int__config_all') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
)

select
    spine.daily_server_id
    , spine.server_id
    , spine.activity_date
    -- OAuth section
    , coalesce(as.is_office365_enabled, os.is_office365_enabled)                                     as is_office365_enabled
    , coalesce(as.is_google_enabled, os.is_google_enabled)                                           as is_google_enabled
    , coalesce(as.is_gitlab_enabled, os.is_gitlab_enabled)                                           as is_gitlab_enabled
    , coalesce(as.is_openid_enabled, os.is_openid_enabled)                                           as is_openid_enabled
    , coalesce(as.is_openid_google_enabled, os.is_openid_google_enabled)                             as is_openid_google_enabled
    , coalesce(as.is_openid_gitlab_enabled, os.is_openid_gitlab_enabled)                             as is_openid_gitlab_enabled
    , coalesce(as.is_openid_office365_enabled, os.is_openid_office365_enabled)                       as is_openid_office365_enabled
    
    -- Ldap section
    , coalesce(as.connection_security_ldap, ls.connection_security_ldap)                             as connection_security_ldap
    , coalesce(as.enable_ldap, ls.enable_ldap)                                                       as enable_ldap
    , coalesce(as.enable_admin_filter ls.enable_admin_filter)                                        as enable_admin_filter
    , coalesce(as.enable_sync, ls.enable_sync)                                                       as enable_sync
    , coalesce(as.isdefault_email_attribute_ldap, ls.isdefault_email_attribute_ldap)                 as isdefault_email_attribute_ldap
    , coalesce(as.isdefault_first_name_attribute_ldap, ls.isdefault_first_name_attribute_ldap)       as isdefault_first_name_attribute_ldap
    , coalesce(as.isdefault_group_display_name_attribute, ls.isdefault_group_display_name_attribute) as isdefault_group_display_name_attribute
    , coalesce(as.isdefault_group_id_attribute, ls.isdefault_group_id_attribute)                     as isdefault_group_id_attribute
    , coalesce(as.isdefault_id_attribute_ldap, ls.isdefault_id_attribute_ldap)                       as isdefault_id_attribute_ldap
    , coalesce(as.isdefault_last_name_attribute_ldap, ls.isdefault_last_name_attribute_ldap)         as isdefault_last_name_attribute_ldap
    , coalesce(as.isdefault_login_button_border_color_ldap,
               ls.isdefault_login_button_border_color_ldap)                                          as isdefault_login_button_border_color_ldap
    , coalesce(as.isdefault_login_button_color_ldap, ls.isdefault_login_button_color_ldap)           as isdefault_login_button_color_ldap
    , coalesce(as.isdefault_login_button_text_color_ldap, ls.isdefault_login_button_text_color_ldap) as isdefault_login_button_text_color_ldap
    , coalesce(as.isdefault_login_field_name, ls.isdefault_login_field_name)                         as isdefault_login_field_name
    , coalesce(as.isdefault_login_id_attribute, ls.isdefault_login_id_attribute)                     as isdefault_login_id_attribute
    , coalesce(as.isdefault_nickname_attribute_ldap, ls.isdefault_nickname_attribute_ldap)           as isdefault_nickname_attribute_ldap
    , coalesce(as.isdefault_position_attribute_ldap, ls.isdefault_position_attribute_ldap)           as isdefault_position_attribute_ldap
    , coalesce(as.isdefault_username_attribute_ldap, ls.isdefault_username_attribute_ldap)           as isdefault_username_attribute_ldap
    , coalesce(as.isempty_admin_filter, ls.isempty_admin_filter)                                     as isempty_admin_filter
    , coalesce(as.isempty_group_filter, ls.isempty_group_filter)                                     as isempty_group_filter
    , coalesce(as.isempty_guest_filter, ls.isempty_guest_filter)                                     as isempty_guest_filter
    , coalesce(as.isnotempty_picture_attribute, ls.isnotempty_picture_attribute)                     as isnotempty_picture_attribute
    , coalesce(as.isnotempty_private_key, ss.isnotempty_private_key)                                 as isnotempty_private_key
    , coalesce(as.isnotempty_public_certificate, s.isnotempty_public_certificate)                    as isnotempty_public_certificate
    , coalesce(as.max_page_size, ls.max_page_size)                                                   as max_page_size
    , coalesce(as.query_timeout_ldap, ls.query_timeout_ldap)                                         as query_timeout_ldap
    , coalesce(as.segment_dedupe_id_ldap, ss.segment_dedupe_id_ldap)                                 as segment_dedupe_id_ldap
    , coalesce(as.skip_certificate_verification, ls.skip_certificate_verification)                   as skip_certificate_verification
    , coalesce(as.sync_interval_minutes, ls.sync_interval_minutes)                                   as sync_interval_minutes
    -- Saml section
    , coalesce(as.enable_saml, ss.enable_saml)                                                       as enable_saml
    , coalesce(as.enable_admin_attribute, ss.enable_admin_attribute)                                 as enable_admin_attribute
    , coalesce(as.enable_sync_with_ldap, ss.enable_sync_with_ldap)                                   as enable_sync_with_ldap
    , coalesce(as.enable_sync_with_ldap_include_auth, ss.enable_sync_with_ldap_include_auth)         as enable_sync_with_ldap_include_auth
    , coalesce(as.encrypt_saml, ss.encrypt_saml)                                                     as encrypt_saml
    , coalesce(as.ignore_guests_ldap_sync, ss.ignore_guests_ldap_sync)                               as ignore_guests_ldap_sync
    , coalesce(as.isdefault_admin_attribute, ss.isdefault_admin_attribute)                           as isdefault_admin_attribute
    , coalesce(as.isdefault_canonical_algorithm, ss.isdefault_canonical_algorithm)                   as isdefault_canonical_algorithm
    , coalesce(as.isdefault_email_attribute_saml, ss.isdefault_email_attribute_saml)                 as isdefault_email_attribute_saml
    , coalesce(as.isdefault_first_name_attribute_saml, ss.isdefault_first_name_attribute_saml)       as isdefault_first_name_attribute_saml
    , coalesce(as.isdefault_guest_attribute, ss.isdefault_guest_attribute)                           as isdefault_guest_attribute
    , coalesce(as.isdefault_id_attribute_saml, ss.isdefault_id_attribute_saml)                       as isdefault_id_attribute_saml
    , coalesce(as.isdefault_last_name_attribute_saml, ss.isdefault_last_name_attribute_saml)         as isdefault_last_name_attribute_saml
    , coalesce(as.isdefault_locale_attribute, ss.isdefault_locale_attribute)                         as isdefault_locale_attribute
    , coalesce(as.isdefault_login_button_border_color_saml,
               ss.isdefault_login_button_border_color_saml)                                          as isdefault_login_button_border_color_saml
    , coalesce(as.isdefault_login_button_color_saml, ss.isdefault_login_button_color_saml)           as isdefault_login_button_color_saml
    , coalesce(as.isdefault_login_button_text, ss.isdefault_login_button_text)                       as isdefault_login_button_text
    , coalesce(as.isdefault_login_button_text_color_saml, ss.isdefault_login_button_text_color_saml) as isdefault_login_button_text_color_saml
    , coalesce(as.isdefault_nickname_attribute_saml, ss.isdefault_nickname_attribute_saml)           as isdefault_nickname_attribute_saml
    , coalesce(as.isdefault_position_attribute_saml, ss.isdefault_position_attribute_saml)           as isdefault_position_attribute_saml
    , coalesce(as.isdefault_scoping_idp_name, ss.isdefault_scoping_idp_name)                         as isdefault_scoping_idp_name
    , coalesce(as.isdefault_scoping_idp_provider_id, ss.isdefault_scoping_idp_provider_id)           as isdefault_scoping_idp_provider_id
    , coalesce(as.isdefault_signature_algorithm, ss.isdefault_signature_algorithm)                   as isdefault_signature_algorithm
    , coalesce(as.isdefault_username_attribute_saml, ss.isdefault_username_attribute_saml)           as isdefault_username_attribute_saml
    , coalesce(as.sign_request, ss.sign_request)                                                     as sign_request
    , coalesce(as.verify_saml, ss.verify_saml)                                                       as verify_saml    
    -- Plugin section
    , coalesce(as.allow_insecure_download_url, 
               ps.allow_insecure_download_url) as allow_insecure_download_url
    , coalesce(as.automatic_prepackaged_plugins, ps.automatic_prepackaged_plugins)
    , coalesce(as.chimera_oauth_proxy_url, ps.chimera_oauth_proxy_url) as chimera_oauth_proxy_url
    , coalesce(as.enable_plugin, ps.enable_plugin) as enable_plugin
    , coalesce(as.enable_alertmanager, ps.enable_alertmanager) as enable_alertmanager
    , coalesce(as.enable_antivirus, ps.enable_antivirus) as enable_antivirus
    , coalesce(as.enable_autolink, ps.enable_autolink) as enable_autolink
    , coalesce(as.enable_aws_sns, ps.enable_aws_sns) as enable_aws_sns
    , coalesce(as.enable_bitbucket, ps.enable_bitbucket) as enable_bitbucket
    , coalesce(as.enable_channel_export, ps.enable_channel_export) as enable_channel_export
    , coalesce(as.enable_circleci, ps.enable_circleci) as enable_circleci
    , coalesce(as.enable_confluence, ps.enable_confluence) as enable_confluence
    , coalesce(as.enable_custom_user_attributes, 
               ps.enable_custom_user_attributes) as enable_custom_user_attributes
    , coalesce(as.enable_diceroller, ps.enable_diceroller) as enable_diceroller
    , coalesce(as.enable_digitalocean, ps.enable_digitalocean) as enable_digitalocean
    , coalesce(as.enable_focalboard, ps.enable_focalboard) as enable_focalboard
    , coalesce(as.enable_giphy, ps.enable_giphy) as enable_giphy
    , coalesce(as.enable_github, ps.enable_github) as enable_github
    , coalesce(as.enable_gitlab, ps.enable_gitlab) as enable_gitlab
    , coalesce(as.enable_health_check, ps.enable_health_check) as enable_health_check
    , coalesce(as.enable_icebreaker, ps.enable_icebreaker) as enable_icebreaker
    , coalesce(as.enable_incident_management, ps.enable_incident_management) as enable_incident_management
    , coalesce(as.enable_incident_response, ps.enable_incident_response) as enable_incident_response
    , coalesce(as.enable_jenkins, ps.enable_jenkins) as enable_jenkins
    , coalesce(as.enable_jespino_recommend, ps.enable_jespino_recommend) as enable_jespino_recommend
    , coalesce(as.enable_jira, ps.enable_jira) as enable_jira
    , coalesce(as.enable_jitsi, ps.enable_jitsi) as enable_jitsi
    , coalesce(as.enable_marketplace, ps.enable_marketplace) as enable_marketplace
    , coalesce(as.enable_matterpoll, ps.enable_matterpoll) as enable_matterpoll
    , coalesce(as.enable_mattermost_agenda, 
               ps.enable_mattermost_agenda) as enable_mattermost_agenda
    , coalesce(as.enable_mattermost_apps, ps.enable_mattermost_apps) as enable_mattermost_apps
    , coalesce(as.enable_mattermost_azure_devops, 
               ps.enable_mattermost_azure_devops) as enable_mattermost_azure_devops
    , coalesce(as.enable_mattermost_calls, ps.enable_mattermost_calls) as enable_mattermost_calls
    , coalesce(as.enable_mattermost_hackerone, 
               ps.enable_mattermost_hackerone) as enable_mattermost_hackerone
    , coalesce(as.enable_mattermost_msteams_meetings, 
               ps.enable_mattermost_msteams_meetings) as enable_mattermost_msteams_meetings
    , coalesce(as.enable_mattermost_msteams_sync, 
               ps.enable_mattermost_msteams_sync) as enable_mattermost_msteams_sync
    , coalesce(as.enable_mattermost_profanity_filter, 
               ps.enable_mattermost_profanity_filter) as enable_mattermost_profanity_filter
    , coalesce(as.enable_mattermost_servicenow, ps.enable_mattermost_servicenow) as enable_mattermost_servicenow
    , coalesce(as.enable_mattermost_servicenow_virtual_agent, 
               ps.enable_mattermost_servicenow_virtual_agent) as enable_mattermost_servicenow_virtual_agent
    , coalesce(as.enable_memes, ps.enable_memes) as enable_memes
    , coalesce(as.enable_mscalendar, ps.enable_mscalendar) as enable_mscalendar
    , coalesce(as.enable_nps, ps.enable_nps) as enable_nps
    , coalesce(as.enable_nps_survey, ps.enable_nps_survey) as enable_nps_survey
    , coalesce(as.enable_playbooks, ps.enable_playbooks) as enable_playbooks
    , coalesce(as.enable_remote_marketplace, 
               ps.enable_remote_marketplace) as enable_remote_marketplace
    , coalesce(as.enable_ru_loop_plugin_embeds, ps.enable_ru_loop_plugin_embeds) as enable_ru_loop_plugin_embeds
    , coalesce(as.enable_ru_loop_plugin_scheduler, ps.enable_ru_loop_plugin_scheduler) as enable_ru_loop_plugin_scheduler
    , coalesce(as.enable_ru_loop_plugin_user_fields, ps.enable_ru_loop_plugin_user_fields) as enable_ru_loop_plugin_user_fields
    , coalesce(as.enable_set_default_theme, ps.enable_set_default_theme) as enable_set_default_theme
    , coalesce(as.enable_skype4business, ps.enable_skype4business) as enable_skype4business
    , coalesce(as.enable_todo, ps.enable_todo) as enable_todo
    , coalesce(as.enable_uploads, ps.enable_uploads) as enable_uploads
    , coalesce(as.enable_webex, ps.enable_webex) as enable_webex
    , coalesce(as.enable_welcome_bot, ps.enable_welcome_bot) as enable_welcome_bot
    , coalesce(as.enable_zoom, ps.enable_zoom) as enable_zoom
    , coalesce(as.is_default_marketplace_url, ps.is_default_marketplace_url) as is_default_marketplace_url
    , coalesce(as.require_plugin_signature, ps.require_plugin_signature) as require_plugin_signature
    , coalesce(as.signature_public_key_files, ps.signature_public_key_files) as signature_public_key_files
    , coalesce(as.version_alertmanager, ps.version_alertmanager) as version_alertmanager
    , coalesce(as.version_antivirus, ps.version_antivirus) as version_antivirus
    , coalesce(as.version_autolink, ps.version_autolink) as version_autolink
    , coalesce(as.version_aws_sns, ps.version_aws_sns) as version_aws_sns
    , coalesce(as.version_bitbucket, ps.version_bitbucket) as version_bitbucket
    , coalesce(as.version_channel_export, ps.version_channel_export) as version_channel_export
    , coalesce(as.version_circleci, ps.version_circleci) as version_circleci
    , coalesce(as.version_confluence, ps.version_confluence) as version_confluence
    , coalesce(as.version_custom_user_attributes, 
               ps.version_custom_user_attributes) as version_custom_user_attributes
    , coalesce(as.version_diceroller, ps.version_diceroller) as version_diceroller
    , coalesce(as.version_digitalocean, ps.version_digitalocean) as version_digitalocean
    , coalesce(as.version_giphy, ps.version_giphy) as version_giphy
    , coalesce(as.version_github, ps.version_github) as version_github
    , coalesce(as.version_gitlab, ps.version_gitlab) as version_gitlab
    , coalesce(as.version_icebreaker, ps.version_icebreaker) as version_icebreaker
    , coalesce(as.version_incident_management, ps.version_incident_management) as version_incident_management
    , coalesce(as.version_incident_response, ps.version_incident_response) as version_incident_response
    , coalesce(as.version_jenkins, ps.version_jenkins) as version_jenkins
    , coalesce(as.version_jespino_recommend, ps.version_jespino_recommend) as version_jespino_recommend
    , coalesce(as.version_jira, ps.version_jira) as version_jira
    , coalesce(as.version_jitsi, ps.version_jitsi) as version_jitsi
    , coalesce(as.version_matterpoll, ps.version_matterpoll) as version_matterpoll
    , coalesce(as.version_mattermost_agenda, 
               ps.version_mattermost_agenda) as version_mattermost_agenda
    , coalesce(as.version_mattermost_apps, ps.version_mattermost_apps) as version_mattermost_apps
    , coalesce(as.version_mattermost_azure_devops, 
               ps.version_mattermost_azure_devops) as version_mattermost_azure_devops
    , coalesce(as.version_mattermost_calls, 
               ps.version_mattermost_calls) as version_mattermost_calls
    , coalesce(as.version_mattermost_hackerone, 
               ps.version_mattermost_hackerone) as version_mattermost_hackerone
    , coalesce(as.version_mattermost_msteams_meetings, 
               ps.version_mattermost_msteams_meetings) as version_mattermost_msteams_meetings
    , coalesce(as.version_mattermost_msteams_sync, 
               ps.version_mattermost_msteams_sync) as version_mattermost_msteams_sync
    , coalesce(as.version_mattermost_profanity_filter, 
               ps.version_mattermost_profanity_filter) as version_mattermost_profanity_filter
    , coalesce(as.version_mattermost_servicenow, ps.version_mattermost_servicenow) as version_mattermost_servicenow
    , coalesce(as.version_mattermost_servicenow_virtual_agent, 
               ps.version_mattermost_servicenow_virtual_agent) as version_mattermost_servicenow_virtual_agent
    , coalesce(as.version_memes, ps.version_memes) as version_memes
    , coalesce(as.version_mscalendar, ps.version_mscalendar) as version_mscalendar
    , coalesce(as.version_nps, ps.version_nps) as version_nps
    , coalesce(as.version_playbooks, ps.version_playbooks) as version_playbooks
    , coalesce(as.version_set_default_theme, ps.version_set_default_theme) as version_set_default_theme
    , coalesce(as.version_todo, ps.version_todo) as version_todo
    , coalesce(as.version_webex, ps.version_webex) as version_webex
    , coalesce(as.version_welcome_bot, ps.version_welcome_bot) as version_welcome_bot
    , coalesce(as.version_zoom, ps.version_zoom) as version_zoom
    -- Service section
    , coalesce(as.allow_cookies_for_subdomains,
               vs.allow_cookies_for_subdomains) as allow_cookies_for_subdomains
    , coalesce(as.allow_edit_post_service,
               vs.allow_edit_post_service) as allow_edit_post_service
    , coalesce(as.allow_persistent_notifications,
               vs.allow_persistent_notifications) as allow_persistent_notifications
    , coalesce(as.allow_persistent_notifications_for_guests,
               vs.allow_persistent_notifications_for_guests) as allow_persistent_notifications_for_guests
    , coalesce(as.allow_synced_drafts, vs.allow_synced_drafts) as allow_synced_drafts
    , coalesce(as.close_unused_direct_messages, vs.close_unused_direct_messages) as close_unused_direct_messages
    , coalesce(as.cluster_log_timeout_milliseconds, vs.cluster_log_timeout_milliseconds) as cluster_log_timeout_milliseconds
    , coalesce(as.collapsed_threads, vs.collapsed_threads) as collapsed_threads
    , coalesce(as.connection_security_service, vs.connection_security_service) as connection_security_service
    , coalesce(as.cors_allow_credentials, vs.cors_allow_credentials) as cors_allow_credentials
    , coalesce(as.cors_debug, vs.cors_debug) as cors_debug
    , coalesce(as.custom_cert_header, vs.custom_cert_header) as custom_cert_header
    , coalesce(as.custom_service_terms_enabled, vs.custom_service_terms_enabled) as custom_service_terms_enabled
    , coalesce(as.default_team_name, vs.default_team_name) as default_team_name
    , coalesce(as.developer_flags, vs.developer_flags) as developer_flags
    , coalesce(as.disable_bots_when_owner_is_deactivated,
               vs.disable_bots_when_owner_is_deactivated) as disable_bots_when_owner_is_deactivated
    , coalesce(as.disable_legacy_mfa, vs.disable_legacy_mfa) as disable_legacy_mfa
    , coalesce(as.enable_api_channel_deletion, vs.enable_api_channel_deletion) as enable_api_channel_deletion
    , coalesce(as.enable_apiv3, vs.enable_apiv3) as enable_apiv3
    , coalesce(as.enable_api_post_deletion, vs.enable_api_post_deletion) as enable_api_post_deletion
    , coalesce(as.enable_api_team_deletion, vs.enable_api_team_deletion) as enable_api_team_deletion
    , coalesce(as.enable_api_trigger_admin_notification,
               vs.enable_api_trigger_admin_notification) as enable_api_trigger_admin_notification
    , coalesce(as.enable_api_user_deletion, vs.enable_api_user_deletion) as enable_api_user_deletion
    , coalesce(as.enable_bot_account_creation,
               vs.enable_bot_account_creation) as enable_bot_account_creation
    , coalesce(as.enable_channel_viewed_messages_service,
               vs.enable_channel_viewed_messages_service) as enable_channel_viewed_messages_service
    , coalesce(as.enable_commands_service, vs.enable_commands_service) as enable_commands_service
    , coalesce(as.enable_custom_emoji_service, vs.enable_custom_emoji_service) as enable_custom_emoji_service
    , coalesce(as.enable_developer_service, vs.enable_developer_service) as enable_developer_service
    , coalesce(as.enable_email_invitations, vs.enable_email_invitations) as enable_email_invitations
    , coalesce(as.enable_emoji_picker_service, vs.enable_emoji_picker_service) as enable_emoji_picker_service
    , coalesce(as.enable_file_search, vs.enable_file_search) as enable_file_search
    , coalesce(as.enable_gif_picker, vs.enable_gif_picker) as enable_gif_picker
    , coalesce(as.enable_incoming_webhooks_service,
               vs.enable_incoming_webhooks_service) as enable_incoming_webhooks_service
    , coalesce(as.enable_insecure_outgoing_connections_service, 
               vs.enable_insecure_outgoing_connections_service) as enable_insecure_outgoing_connections_service
    , coalesce(as.enable_latex, vs.enable_latex) as enable_latex
    , coalesce(as.enable_legacy_sidebar, vs.enable_legacy_sidebar) as enable_legacy_sidebar
    , coalesce(as.enable_link_previews, vs.enable_link_previews) as enable_link_previews
    , coalesce(as.enable_local_mode, vs.enable_local_mode) as enable_local_mode
    , coalesce(as.enable_multifactor_authentication_service,
               vs.enable_multifactor_authentication_service) as enable_multifactor_authentication_service
    , coalesce(as.enable_oauth_service_provider_service,
               vs.enable_oauth_service_provider_service) as enable_oauth_service_provider_service
    , coalesce(as.enable_onboarding_flow, vs.enable_onboarding_flow) as enable_onboarding_flow
    , coalesce(as.enable_only_admin_integrations_service,
               vs.enable_only_admin_integrations_service) as enable_only_admin_integrations_service
    , coalesce(as.enable_opentracing, vs.enable_opentracing) as enable_opentracing
    , coalesce(as.enable_outgoing_oauth_connections,
               vs.enable_outgoing_oauth_connections) as enable_outgoing_oauth_connections
    , coalesce(as.enable_outgoing_webhooks, vs.enable_outgoing_webhooks) as enable_outgoing_webhooks
    , coalesce(as.enable_permalink_previews, vs.enable_permalink_previews) as enable_permalink_previews
    , coalesce(as.enable_post_icon_override, vs.enable_post_icon_override) as enable_post_icon_override
    , coalesce(as.enable_post_search, vs.enable_post_search) as enable_post_search
    , coalesce(as.enable_post_username_override, vs.enable_post_username_override) as enable_post_username_override
    , coalesce(as.enable_preview_features, vs.enable_preview_features) as enable_preview_features
    , coalesce(as.enable_security_fix_alert, vs.enable_security_fix_alert) as enable_security_fix_alert
    , coalesce(as.enable_svgs, vs.enable_svgs) as enable_svgs
    , coalesce(as.enable_testing, vs.enable_testing) as enable_testing
    , coalesce(as.enable_tutorial, vs.enable_tutorial) as enable_tutorial
    , coalesce(as.enable_user_access_tokens, vs.enable_user_access_tokens) as enable_user_access_tokens
    , coalesce(as.enable_user_statuses, vs.enable_user_statuses) as enable_user_statuses
    , coalesce(as.enable_user_typing_messages, vs.enable_user_typing_messages) as enable_user_typing_messages
    , coalesce(as.enable_user_typing_messages,
               vs.enforce_multifactor_authentication_service) as enable_user_typing_messages
    , coalesce(as.experimental_channel_organization,
               vs.experimental_channel_organization) as experimental_channel_organization
    , coalesce(as.experimental_channel_sidebar_organization,
               vs.experimental_channel_sidebar_organization) as experimental_channel_sidebar_organization
    , coalesce(as.experimental_data_prefetch vs.experimental_data_prefetch) as experimental_data_prefetch
    , coalesce(as.experimental_enable_authentication_transfer,
               vs.experimental_enable_authentication_transfer) as experimental_enable_authentication_transfer
    , coalesce(as.experimental_enable_default_channel_leave_join_messages,
               vs.experimental_enable_default_channel_leave_join_messages) as experimental_enable_default_channel_leave_join_messages
    , coalesce(as.experimental_enable_hardened_mode,
               vs.experimental_enable_hardened_mode) as experimental_enable_hardened_mode
    , coalesce(as.experimental_group_unread_channels, 
               vs.experimental_group_unread_channels) as experimental_group_unread_channels
    , coalesce(as.experimental_ldap_group_sync, 
               vs.experimental_ldap_group_sync) as experimental_ldap_group_sync
    , coalesce(as.experimental_limit_client_config, 
               vs.experimental_limit_client_config) as experimental_limit_client_config
    , coalesce(as.experimental_strict_csrf_enforcement, 
               vs.experimental_strict_csrf_enforcement) as experimental_strict_csrf_enforcement
    , coalesce(as.extend_session_length_with_activity, 
               vs.extend_session_length_with_activity) as extend_session_length_with_activity
    , coalesce(as.forward_80_to_443, vs.forward_80_to_443) as forward_80_to_443
    , coalesce(as.gfycat_api_key, vs.gfycat_api_key) as gfycat_api_key
    , coalesce(as.gfycat_api_secret, vs.gfycat_api_secret) as gfycat_api_secret
    , coalesce(as.isdefault_allowed_untrusted_internal_connections,
               vs.isdefault_allowed_untrusted_internal_connections) as isdefault_allowed_untrusted_internal_connections
    , coalesce(as.isdefault_allow_cors_from, vs.isdefault_allow_cors_from) as isdefault_allow_cors_from
    , coalesce(as.isdefault_cors_exposed_headers, vs.isdefault_cors_exposed_headers) as isdefault_cors_exposed_headers
    , coalesce(as.isdefault_google_developer_key, vs.isdefault_google_developer_key) as isdefault_google_developer_key
    , coalesce(as.isdefault_idle_timeout, vs.isdefault_idle_timeout) as isdefault_idle_timeout
    , coalesce(as.isdefault_image_proxy_options, vs.isdefault_image_proxy_options) as isdefault_image_proxy_options
    , coalesce(as.isdefault_image_proxy_type, vs.isdefault_image_proxy_type) as isdefault_image_proxy_type
    , coalesce(as.isdefault_image_proxy_url, vs.isdefault_image_proxy_url) as isdefault_image_proxy_url
    , coalesce(as.isdefault_read_timeout, vs.isdefault_read_timeout) as isdefault_read_timeout
    , coalesce(as.isdefault_site_url, vs.isdefault_site_url) as isdefault_site_url
    , coalesce(as.isdefault_tls_cert_file, vs.isdefault_tls_cert_file) as isdefault_tls_cert_file
    , coalesce(as.isdefault_tls_key_file, vs.isdefault_tls_key_file) as isdefault_tls_key_file
    , coalesce(as.isdefault_write_timeout, vs.isdefault_write_timeout) as isdefault_write_timeout
    , coalesce(as.limit_load_search_result, vs.limit_load_search_result) as limit_load_search_result
    , coalesce(as.login_with_certificate, vs.login_with_certificate) as login_with_certificate
    , coalesce(as.managed_resource_paths, vs.managed_resource_paths) as managed_resource_paths
    , coalesce(as.maximum_login_attempts, vs.maximum_login_attempts) as maximum_login_attempts
    , coalesce(as.maximum_payload_size, vs.maximum_payload_size) as maximum_payload_size
    , coalesce(as.maximum_url_length, vs.maximum_url_length) as maximum_url_length
    , coalesce(as.minimum_hashtag_length, vs.minimum_hashtag_length) as minimum_hashtag_length
    , coalesce(as.outgoing_integrations_requests_timeout,
               vs.outgoing_integrations_requests_timeout) as outgoing_integrations_requests_timeout
    , coalesce(as.persistent_notification_interval_minutes,
               vs.persistent_notification_interval_minutes) as persistent_notification_interval_minutes
    , coalesce(as.persistent_notification_max_count, 
               vs.persistent_notification_max_count) as persistent_notification_max_count
    , coalesce(as.persistent_notification_max_recipients, 
               vs.persistent_notification_max_recipients) as persistent_notification_max_recipients
    , coalesce(as.post_edit_time_limit, vs.post_edit_time_limit) as post_edit_time_limit
    , coalesce(as.post_priority, vs.post_priority) as post_priority
    , coalesce(as.refresh_post_stats_run_time, vs.refresh_post_stats_run_time) as refresh_post_stats_run_time
    , coalesce(as.restrict_custom_emoji_creation, vs.restrict_custom_emoji_creation) as restrict_custom_emoji_creation
    , coalesce(as.restrict_link_previews, vs.restrict_link_previews) as restrict_link_previews
    , coalesce(as.restrict_post_delete, vs.restrict_post_delete) as restrict_post_delete
    , coalesce(as.self_hosted_expansion, vs.self_hosted_expansion) as self_hosted_expansion
    , coalesce(as.self_hosted_purchase, vs.self_hosted_purchase) as self_hosted_purchase
    , coalesce(as.session_cache_in_minutes, vs.session_cache_in_minutes) as session_cache_in_minutes
    , coalesce(as.session_idle_timeout_in_minutes, vs.session_idle_timeout_in_minutes) as session_idle_timeout_in_minutes
    , coalesce(as.session_length_mobile_in_days, vs.session_length_mobile_in_days) as session_length_mobile_in_days
    , coalesce(as.session_length_mobile_in_hours, vs.session_length_mobile_in_hours) as session_length_mobile_in_hours
    , coalesce(as.session_length_sso_in_days, vs.session_length_sso_in_days) as session_length_sso_in_days
    , coalesce(as.session_length_sso_in_hours, vs.session_length_sso_in_hours) as session_length_sso_in_hours
    , coalesce(as.session_length_web_in_days, vs.session_length_web_in_days) as session_length_web_in_days
    , coalesce(as.session_length_web_in_hours, vs.session_length_web_in_hours) as session_length_web_in_hours
    , coalesce(as.skip_login_page, vs.skip_login_page) as skip_login_page
    , coalesce(as.terminate_sessions_on_password_change, vs.terminate_sessions_on_password_change) as terminate_sessions_on_password_change
    , coalesce(as.thread_auto_follow, vs.thread_auto_follow) as thread_auto_follow
    , coalesce(as.time_between_user_typing_updates_milliseconds,
               vs.time_between_user_typing_updates_milliseconds) as time_between_user_typing_updates_milliseconds
    , coalesce(as.tls_strict_transport, vs.tls_strict_transport) as tls_strict_transport
    , coalesce(as.uses_letsencrypt, vs.uses_letsencrypt) as uses_letsencrypt
    , coalesce(as.websocket_url, vs.websocket_url) as websocket_url
    , coalesce(as.web_server_mode, vs.web_server_mode) as web_server_mode 
    
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
    left join int_config_all as on spine.daily_server_id = as.daily_server_id
