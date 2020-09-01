{{config({
    "materialized": 'incremental',
    "unique_key": 'id',
    "schema": "staging"
  })
}}

WITH server_config_details AS (
SELECT
    s.date
  , s.server_id
  , sactivity.active_users
  , sactivity.active_users_daily
  , sactivity.active_users_monthly
  , sactivity.bot_accounts
  , sactivity.bot_posts_previous_day
  , sactivity.direct_message_channels
  , sactivity.incoming_webhooks
  , sactivity.outgoing_webhooks
  , sactivity.posts
  , sactivity.posts_previous_day
  , sactivity.private_channels
  , sactivity.private_channels_deleted
  , sactivity.public_channels
  , sactivity.public_channels_deleted
  , sactivity.registered_deactivated_users
  , sactivity.registered_inactive_users
  , sactivity.registered_users
  , sactivity.slash_commands
  , sactivity.teams
  , sactivity.used_apiv3
  , sanalytics.isdefault_max_users_for_statistics
  , sannouncement.allow_banner_dismissal
  , sannouncement.enable_banner
  , sannouncement.isdefault_banner_color
  , sannouncement.isdefault_banner_text_color
  , sclient.allow_edit_post                       AS allow_edit_post_client
  , sclient.android_latest_version
  , sclient.android_min_version
  , sclient.desktop_latest_version
  , sclient.desktop_min_version
  , sclient.enable_apiv3                          AS enable_apiv3_client
  , sclient.enable_channel_viewed_messages        AS enable_channel_viewed_messages_client
  , sclient.enable_commands                       AS enable_commands_client
  , sclient.enable_custom_emoji                   AS enable_custom_emoji_client
  , sclient.enable_developer                      AS enable_developer_client
  , sclient.enable_emoji_picker                   AS enable_emoji_picker_client
  , sclient.enable_incoming_webhooks              AS enable_incoming_webhooks_client
  , sclient.enable_insecure_outgoing_connections  AS enable_insecure_outgoing_connections_client
  , sclient.enable_multifactor_authentication     AS enable_multifactor_authentication_client
  , sclient.enable_oauth_service_provider         AS enable_oauth_service_provider_client
  , sclient.enable_only_admin_integrations        AS enable_only_admin_integrations_client
  , sclient.ios_latest_version
  , sclient.ios_min_version
  , scluster.advertise_address
  , scluster.bind_address
  , scluster.enable_cluster
  , scluster.network_interface
  , scluster.read_only_config
  , scluster.use_experimental_gossip
  , scluster.use_ip_address
  , scompliance.enable_compliance
  , scompliance.enable_compliance_daily
  , sdata.message_retention_days
  , sdata.file_retention_days
  , sdata.enable_message_deletion
  , sdata.enable_file_deletion
  , sdisplay.experimental_timezone
  , sdisplay.isdefault_custom_url_schemes
  , selasticsearch.enable_autocomplete
  , selasticsearch.enable_indexing
  , selasticsearch.enable_searching
  , selasticsearch.isdefault_connection_url
  , selasticsearch.isdefault_index_prefix
  , selasticsearch.isdefault_password
  , selasticsearch.isdefault_username
  , selasticsearch.live_indexing_batch_size
  , selasticsearch.skip_tls_verification
  , selasticsearch.sniff
  , selasticsearch.trace                          AS trace_elasticsearch
  , semail.connection_security                    AS connection_security_email
  , semail.email_batching_buffer_size
  , semail.email_notification_contents_type
  , semail.enable_email_batching
  , semail.enable_preview_mode_banner
  , semail.enable_sign_in_with_email
  , semail.enable_sign_in_with_username
  , semail.enable_sign_up_with_email
  , semail.enable_smtp_auth
  , semail.isdefault_feedback_email
  , semail.isdefault_feedback_name
  , semail.isdefault_feedback_organization
  , semail.isdefault_login_button_border_color    AS isdefault_login_button_border_color_email
  , semail.isdefault_login_button_color           AS isdefault_login_button_color_email
  , semail.isdefault_login_button_text_color      AS isdefault_login_button_text_color_email
  , semail.isdefault_reply_to_address
  , semail.push_notification_contents
  , semail.require_email_verification
  , semail.send_email_notifications
  , semail.send_push_notifications
  , semail.skip_server_certificate_verification
  , semail.use_channel_in_email_notifications
  , sexperimental.client_side_cert_enable
  , sexperimental.enable_click_to_reply
  , sexperimental.enable_post_metadata
  , sexperimental.isdefault_client_side_cert_check
  , sexperimental.restrict_system_admin
  , sexperimental.use_new_saml_library
  , sextension.enable_experimental_extensions
  , sfile.amazon_s3_signv2
  , sfile.amazon_s3_sse
  , sfile.amazon_s3_ssl
  , sfile.amazon_s3_trace
  , sfile.driver_name                             AS driver_name_file
  , sfile.enable_file_attachments
  , sfile.enable_mobile_download
  , sfile.enable_mobile_upload
  , sfile.enable_public_links
  , sfile.isabsolute_directory
  , sfile.isdefault_directory
  , sfile.max_file_size
  , sfile.preview_height
  , sfile.preview_width
  , sfile.profile_height
  , sfile.profile_width
  , sfile.thumbnail_height
  , sfile.thumbnail_width
  , sguest.allow_email_accounts
  , sguest.enable_guest_accounts
  , sguest.enforce_multifactor_authentication     AS enforce_multifactor_authentication_guest
  , sguest.isdefault_restrict_creation_to_domains
  , simage.enable_image_proxy
  , simage.image_proxy_type
  , simage.isdefault_remote_image_proxy_options
  , simage.isdefault_remote_image_proxy_url
  , sldap.connection_security                     AS connection_security_ldap
  , sldap.enable_ldap
  , sldap.enable_admin_filter
  , sldap.enable_sync
  , sldap.isdefault_email_attribute               AS isdefault_email_attribute_ldap
  , sldap.isdefault_first_name_attribute          AS isdefault_first_name_attribute_ldap
  , sldap.isdefault_group_display_name_attribute
  , sldap.isdefault_group_id_attribute
  , sldap.isdefault_id_attribute                  AS isdefault_id_attribute_ldap
  , sldap.isdefault_last_name_attribute           AS isdefault_last_name_attribute_ldap
  , sldap.isdefault_login_button_border_color     AS isdefault_login_button_border_color_ldap
  , sldap.isdefault_login_button_color            AS isdefault_login_button_color_ldap
  , sldap.isdefault_login_button_text_color       AS isdefault_login_button_text_color_ldap
  , sldap.isdefault_login_field_name
  , sldap.isdefault_login_id_attribute
  , sldap.isdefault_nickname_attribute            AS isdefault_nickname_attribute_ldap
  , sldap.isdefault_position_attribute            AS isdefault_position_attribute_ldap
  , sldap.isdefault_username_attribute            AS isdefault_username_attribute_ldap
  , sldap.isempty_admin_filter
  , sldap.isempty_group_filter
  , sldap.isempty_guest_filter
  , sldap.max_page_size
  , sldap.query_timeout                           AS query_timeout_ldap
  , sldap.segment_dedupe_id                       AS segment_dedupe_id_ldap
  , sldap.skip_certificate_verification
  , sldap.sync_interval_minutes
  , slicense.license_id
  , slicense.start_date
  , slicense.edition
  , slicense.expire_date
  , slicense.feature_cluster
  , slicense.feature_compliance
  , slicense.feature_custom_brand
  , slicense.feature_custom_permissions_schemes
  , slicense.feature_data_retention
  , slicense.feature_elastic_search
  , slicense.feature_email_notification_contents
  , slicense.feature_future
  , slicense.feature_google
  , slicense.feature_guest_accounts
  , slicense.feature_guest_accounts_permissions
  , slicense.feature_id_loaded
  , slicense.feature_ldap
  , slicense.feature_ldap_groups
  , slicense.feature_lock_teammate_name_display
  , slicense.feature_message_export
  , slicense.feature_metrics
  , slicense.feature_mfa
  , slicense.feature_mhpns
  , slicense.feature_office365
  , slicense.feature_password
  , slicense.feature_saml
  , slicense.issued_date
  , slicense.users
  , slocalization.available_locales
  , slocalization.default_client_locale
  , slocalization.default_server_locale
  , slog.console_json                             AS console_json_log
  , slog.console_level                            AS console_level_log
  , slog.enable_console                           AS enable_console_log
  , slog.enable_file                              AS enable_file_log
  , slog.enable_webhook_debugging
  , slog.file_json                                AS file_json_log
  , slog.file_level                               AS file_level_log
  , slog.isdefault_file_format
  , slog.isdefault_file_location                  AS isdefault_file_location_log
  , smessage.batch_size
  , smessage.daily_run_time
  , smessage.enable_message_export
  , smessage.export_format
  , smessage.global_relay_customer_type
  , smessage.is_default_global_relay_email_address
  , smessage.is_default_global_relay_smtp_password
  , smessage.is_default_global_relay_smtp_username
  , smetric.block_profile_rate
  , smetric.enable_metrics
  , snativeapp.isdefault_android_app_download_link
  , snativeapp.isdefault_app_download_link
  , snativeapp.isdefault_iosapp_download_link
  , snotifications.console_json                   AS console_json_notifications
  , snotifications.console_level                  AS console_level_notifications
  , snotifications.enable_console                 AS enable_console_notifications
  , snotifications.enable_file                    AS enable_file_notifications
  , snotifications.file_json                      AS file_json_notifications
  , snotifications.file_level                     AS file_level_notifications
  , snotifications.isdefault_file_location        AS isdefault_file_location_notifications
  , soauth.enable_office365_oauth
  , soauth.enable_google_oauth
  , soauth.enable_gitlab_oauth
  , spassword.enable_lowercase
  , spassword.enable_uppercase
  , spassword.enable_symbol
  , spassword.enable_number
  , spassword.password_minimum_length
  , spermissions.phase_1_migration_complete
  , spermissions.phase_2_migration_complete
  , sspermissions.channel_admin_permissions
  , sspermissions.channel_guest_permissions
  , sspermissions.channel_user_permissions
  , sspermissions.system_admin_permissions
  , sspermissions.system_user_permissions
  , sspermissions.team_admin_permissions
  , sspermissions.team_guest_permissions
  , sspermissions.team_user_permissions
  , splugin.allow_insecure_download_url
  , splugin.automatic_prepackaged_plugins
  , splugin.enable_plugins
  , splugin.enable_antivirus
  , splugin.enable_autolink
  , splugin.enable_aws_sns
  , splugin.enable_custom_user_attributes
  , splugin.enable_github
  , splugin.enable_gitlab
  , splugin.enable_health_check
  , splugin.enable_jenkins
  , splugin.enable_jira
  , splugin.enable_marketplace
  , splugin.enable_nps
  , splugin.enable_nps_survey
  , splugin.enable_remote_marketplace
  , splugin.enable_uploads
  , splugin.enable_webex
  , splugin.enable_welcome_bot
  , splugin.enable_zoom
  , splugin.is_default_marketplace_url
  , splugin.require_plugin_signature
  , splugin.signature_public_key_files
  , splugin.version_antivirus
  , splugin.version_autolink
  , splugin.version_aws_sns
  , splugin.version_custom_user_attributes
  , splugin.version_github
  , splugin.version_gitlab
  , splugin.version_jenkins
  , splugin.version_jira
  , splugin.version_nps
  , splugin.version_webex
  , splugin.version_welcome_bot
  , splugin.version_zoom
  , splugins.active_backend_plugins
  , splugins.active_plugins
  , splugins.active_webapp_plugins
  , splugins.disabled_backend_plugins
  , splugins.disabled_plugins
  , splugins.disabled_webapp_plugins
  , splugins.enabled_backend_plugins
  , splugins.enabled_plugins
  , splugins.enabled_webapp_plugins
  , splugins.inactive_backend_plugins
  , splugins.inactive_plugins
  , splugins.inactive_webapp_plugins
  , splugins.plugins_with_broken_manifests
  , splugins.plugins_with_settings
  , sprivacy.show_email_address
  , sprivacy.show_full_name
  , srate.enable_rate_limiter
  , srate.isdefault_vary_by_header
  , srate.max_burst
  , srate.memory_store_size
  , srate.per_sec
  , srate.vary_by_remote_address
  , srate.vary_by_user
  , ssaml.enable_saml
  , ssaml.enable_admin_attribute
  , ssaml.enable_sync_with_ldap
  , ssaml.enable_sync_with_ldap_include_auth
  , ssaml.encrypt_saml
  , ssaml.isdefault_admin_attribute
  , ssaml.isdefault_canonical_algorithm
  , ssaml.isdefault_email_attribute               AS isdefault_email_attribute_saml
  , ssaml.isdefault_first_name_attribute          AS isdefault_first_name_attribute_saml
  , ssaml.isdefault_guest_attribute
  , ssaml.isdefault_id_attribute                  AS isdefault_id_attribute_saml
  , ssaml.isdefault_last_name_attribute           AS isdefault_last_name_attribute_saml
  , ssaml.isdefault_locale_attribute
  , ssaml.isdefault_login_button_border_color     AS isdefault_login_button_border_color_saml
  , ssaml.isdefault_login_button_color            AS isdefault_login_button_color_saml
  , ssaml.isdefault_login_button_text
  , ssaml.isdefault_login_button_text_color       AS isdefault_login_button_text_color_saml
  , ssaml.isdefault_nickname_attribute            AS isdefault_nickname_attribute_saml
  , ssaml.isdefault_position_attribute            AS isdefault_position_attribute_saml
  , ssaml.isdefault_scoping_idp_name
  , ssaml.isdefault_scoping_idp_provider_id
  , ssaml.isdefault_signature_algorithm
  , ssaml.isdefault_username_attribute            AS isdefault_username_attribute_saml
  , ssaml.sign_request
  , ssaml.verify_saml
  , sservice.allow_cookies_for_subdomains
  , sservice.allow_edit_post                      AS allow_edit_post_service
  , sservice.close_unused_direct_messages
  , sservice.connection_security                  AS connection_security_service
  , sservice.cors_allow_credentials
  , sservice.cors_debug
  , sservice.custom_service_terms_enabled         AS custom_service_terms_enabled_service
  , sservice.disable_bots_when_owner_is_deactivated
  , sservice.disable_legacy_mfa
  , sservice.enable_apiv3                         AS enable_apiv3_service
  , sservice.enable_api_team_deletion
  , sservice.enable_bot_account_creation
  , sservice.enable_channel_viewed_messages       AS enable_channel_viewed_messages_service
  , sservice.enable_commands                      AS enable_commands_service
  , sservice.enable_custom_emoji                  AS enable_custom_emoji_service
  , sservice.enable_developer                     AS enable_developer_service
  , sservice.enable_email_invitations
  , sservice.enable_emoji_picker                  AS enable_emoji_picker_service
  , sservice.enable_gif_picker
  , sservice.enable_incoming_webhooks             AS enable_incoming_webhooks_service
  , sservice.enable_insecure_outgoing_connections AS enable_insecure_outgoing_connections_service
  , sservice.enable_latex
  , sservice.enable_multifactor_authentication    AS enable_multifactor_authentication_service
  , sservice.enable_oauth_service_provider        AS enable_oauth_service_provider_service
  , sservice.enable_only_admin_integrations       AS enable_only_admin_integrations_service
  , sservice.enable_outgoing_webhooks
  , sservice.enable_post_icon_override
  , sservice.enable_post_search
  , sservice.enable_post_username_override
  , sservice.enable_preview_features
  , sservice.enable_security_fix_alert
  , sservice.enable_svgs
  , sservice.enable_testing
  , sservice.enable_tutorial
  , sservice.enable_user_access_tokens
  , sservice.enable_user_statuses
  , sservice.enable_user_typing_messages
  , sservice.enforce_multifactor_authentication   AS enforce_multifactor_authentication_service
  , sservice.experimental_channel_organization
  , sservice.experimental_enable_authentication_transfer
  , sservice.experimental_enable_default_channel_leave_join_messages
  , sservice.experimental_enable_hardened_mode
  , sservice.experimental_group_unread_channels
  , sservice.experimental_ldap_group_sync
  , sservice.experimental_limit_client_config
  , sservice.experimental_strict_csrf_enforcement
  , sservice.forward_80_to_443
  , sservice.gfycat_api_key
  , sservice.gfycat_api_secret
  , sservice.isdefault_allowed_untrusted_internal_connections
  , sservice.isdefault_allowed_untrusted_inteznal_connections
  , sservice.isdefault_allow_cors_from
  , sservice.isdefault_cors_exposed_headers
  , sservice.isdefault_google_developer_key
  , sservice.isdefault_image_proxy_options
  , sservice.isdefault_image_proxy_type
  , sservice.isdefault_image_proxy_url
  , sservice.isdefault_read_timeout
  , sservice.isdefault_site_url
  , sservice.isdefault_tls_cert_file
  , sservice.isdefault_tls_key_file
  , sservice.isdefault_write_timeout
  , sservice.maximum_login_attempts
  , sservice.minimum_hashtag_length
  , sservice.post_edit_time_limit
  , sservice.restrict_custom_emoji_creation
  , sservice.restrict_post_delete
  , sservice.session_cache_in_minutes
  , sservice.session_idle_timeout_in_minutes
  , sservice.session_length_mobile_in_days
  , sservice.session_length_sso_in_days
  , sservice.session_length_web_in_days
  , sservice.tls_strict_transport
  , sservice.uses_letsencrypt
  , sservice.websocket_url
  , sservice.web_server_mode
  , ssql.driver_name                              AS driver_name_sql
  , ssql.enable_public_channels_materialization
  , ssql.max_idle_conns
  , ssql.max_open_conns
  , ssql.query_timeout                            AS query_timeout_sql
  , ssql.trace                                    AS trace_sql
  , ssupport.custom_service_terms_enabled         AS custom_service_terms_enabled_support
  , ssupport.custom_terms_of_service_enabled
  , ssupport.custom_terms_of_service_re_acceptance_period
  , ssupport.isdefault_about_link
  , ssupport.isdefault_help_link
  , ssupport.isdefault_privacy_policy_link
  , ssupport.isdefault_report_a_problem_link
  , ssupport.isdefault_support_email
  , ssupport.isdefault_terms_of_service_link
  , ssupport.segment_dedupe_id                    AS segment_dedupe_id_support
  , steam.enable_confirm_notifications_to_channel
  , steam.enable_custom_brand
  , steam.enable_open_server
  , steam.enable_team_creation
  , steam.enable_user_creation
  , steam.enable_user_deactivation
  , steam.enable_x_to_leave_channels_from_lhs
  , steam.experimental_default_channels
  , steam.experimental_enable_automatic_replies
  , steam.experimental_primary_team
  , steam.experimental_town_square_is_hidden_in_lhs
  , steam.experimental_town_square_is_read_only
  , steam.experimental_view_archived_channels
  , steam.isdefault_custom_brand_text
  , steam.isdefault_custom_description_text
  , steam.isdefault_site_name
  , steam.isdefault_user_status_away_timeout
  , steam.lock_teammate_name_display
  , steam.max_channels_per_team
  , steam.max_notifications_per_channel
  , steam.max_users_per_team
  , steam.restrict_direct_message
  , steam.restrict_private_channel_creation
  , steam.restrict_private_channel_deletion
  , steam.restrict_private_channel_management
  , steam.restrict_private_channel_manage_members
  , steam.restrict_public_channel_creation
  , steam.restrict_public_channel_deletion
  , steam.restrict_public_channel_management
  , steam.restrict_team_invite
  , steam.teammate_name_display
  , steam.view_archived_channels
  , stheme.allowed_themes
  , stheme.allow_custom_themes
  , stheme.enable_theme_selection
  , stheme.isdefault_default_theme
  , stimezone.isdefault_supported_timezones_path
  , swebrtc.enable
  , swebrtc.isdefault_stun_uri
  , swebrtc.isdefault_turn_uri
  , {{ dbt_utils.surrogate_key('s.date', 's.server_id') }} AS id
  , ssql.data_source_replicas
  , ssql.data_source_search_replicas
  , splugin.enable_confluence
  , splugin.enable_jitsi
  , splugin.enable_mscalendar
  , splugin.enable_todo
  , splugin.enable_skype4business
  , splugin.enable_giphy
  , splugin.enable_digital_ocean
  , splugin.enable_incident_response
  , splugin.enable_memes
  , splugin.version_giphy
  , splugin.version_digital_ocean
  , splugin.version_confluence
  , splugin.version_mscalendar
  , splugin.version_incident_response
  , splugin.version_todo
  , splugin.version_memes
  , ssupport.enable_ask_community_link
  , sactivity.guest_accounts
  , scluster.enable_experimental_gossip_encryption
FROM {{ ref('server_daily_details') }}                      s
    LEFT JOIN {{ ref('server_activity_details') }}            sactivity
    ON s.server_id = sactivity.server_id AND s.date = sactivity.date
    LEFT JOIN {{ ref('server_analytics_details') }}           sanalytics
    ON s.server_id = sanalytics.server_id AND s.date = sanalytics.date
    LEFT JOIN {{ ref('server_announcement_details') }}        sannouncement
    ON s.server_id = sannouncement.server_id AND s.date = sannouncement.date
    LEFT JOIN {{ ref('server_client_requirements_details') }} sclient
    ON s.server_id = sclient.server_id AND s.date = sclient.date
    LEFT JOIN {{ ref('server_cluster_details') }}             scluster
    ON s.server_id = scluster.server_id AND s.date = scluster.date
    LEFT JOIN {{ ref('server_compliance_details') }}          scompliance
    ON s.server_id = scompliance.server_id AND s.date = scompliance.date
    LEFT JOIN {{ ref('server_data_retention_details') }}      sdata
    ON s.server_id = sdata.server_id AND s.date = sdata.date
    LEFT JOIN {{ ref('server_display_details') }}             sdisplay
    ON s.server_id = sdisplay.server_id AND s.date = sdisplay.date
    LEFT JOIN {{ ref('server_elasticsearch_details') }}       selasticsearch
    ON s.server_id = selasticsearch.server_id AND s.date = selasticsearch.date
    LEFT JOIN {{ ref('server_email_details') }}               semail
    ON s.server_id = semail.server_id AND s.date = semail.date
    LEFT JOIN {{ ref('server_experimental_details') }}        sexperimental
    ON s.server_id = sexperimental.server_id AND s.date = sexperimental.date
    LEFT JOIN {{ ref('server_extension_details') }}           sextension
    ON s.server_id = sextension.server_id AND s.date = sextension.date
    LEFT JOIN {{ ref('server_file_details') }}                sfile
    ON s.server_id = sfile.server_id AND s.date = sfile.date
    LEFT JOIN {{ ref('server_guest_account_details') }}       sguest
    ON s.server_id = sguest.server_id AND s.date = sguest.date
    LEFT JOIN {{ ref('server_image_proxy_details') }}         simage
    ON s.server_id = simage.server_id AND s.date = simage.date
    LEFT JOIN {{ ref('server_ldap_details') }}                sldap
    ON s.server_id = sldap.server_id AND s.date = sldap.date
    LEFT JOIN {{ ref('server_license_details') }}             slicense
    ON s.server_id = slicense.server_id AND s.date = slicense.date
    LEFT JOIN {{ ref('server_localization_details') }}        slocalization
    ON s.server_id = slocalization.server_id AND s.date = slocalization.date
    LEFT JOIN {{ ref('server_log_details') }}                 slog
    ON s.server_id = slog.server_id AND s.date = slog.date
    LEFT JOIN {{ ref('server_message_export_details') }}      smessage
    ON s.server_id = smessage.server_id AND s.date = smessage.date
    LEFT JOIN {{ ref('server_metric_details') }}              smetric
    ON s.server_id = smetric.server_id AND s.date = smetric.date
    LEFT JOIN {{ ref('server_nativeapp_details') }}           snativeapp
    ON s.server_id = snativeapp.server_id AND s.date = snativeapp.date
    LEFT JOIN {{ ref('server_notifications_log_details') }}   snotifications
    ON s.server_id = snotifications.server_id AND s.date = snotifications.date
    LEFT JOIN {{ ref('server_oauth_details') }}               soauth
    ON s.server_id = soauth.server_id AND s.date = soauth.date
    LEFT JOIN {{ ref('server_password_details') }}            spassword
    ON s.server_id = spassword.server_id AND s.date = spassword.date
    LEFT JOIN {{ ref('server_permissions_general_details') }} spermissions
    ON s.server_id = spermissions.user_id AND s.date = spermissions.date
    LEFT JOIN {{ ref('server_permissions_system_details') }}  sspermissions
    ON s.server_id = sspermissions.user_id AND s.date = sspermissions.date
    LEFT JOIN {{ ref('server_plugin_details') }}              splugin
    ON s.server_id = splugin.server_id AND s.date = splugin.date
    LEFT JOIN {{ ref('server_plugins_details') }}             splugins
    ON s.server_id = splugins.server_id AND s.date = splugins.date
    LEFT JOIN {{ ref('server_privacy_details') }}             sprivacy
    ON s.server_id = sprivacy.server_id AND s.date = sprivacy.date
    LEFT JOIN {{ ref('server_rate_details') }}                srate
    ON s.server_id = srate.server_id AND s.date = srate.date
    LEFT JOIN {{ ref('server_saml_details') }}                ssaml
    ON s.server_id = ssaml.server_id AND s.date = ssaml.date
    LEFT JOIN {{ ref('server_service_details') }}             sservice
    ON s.server_id = sservice.server_id AND s.date = sservice.date
    LEFT JOIN {{ ref('server_sql_details') }}                 ssql
    ON s.server_id = ssql.server_id AND s.date = ssql.date
    LEFT JOIN {{ ref('server_support_details') }}             ssupport
    ON s.server_id = ssupport.server_id AND s.date = ssupport.date
    LEFT JOIN {{ ref('server_team_details') }}                steam
    ON s.server_id = steam.server_id AND s.date = steam.date
    LEFT JOIN {{ ref('server_theme_details') }}               stheme
    ON s.server_id = stheme.server_id AND s.date = stheme.date
    LEFT JOIN {{ ref('server_timezone_details') }}            stimezone
    ON s.server_id = stimezone.server_id AND s.date = stimezone.date
    LEFT JOIN {{ ref('server_webrtc_details') }}              swebrtc
    ON s.server_id = swebrtc.server_id AND s.date = swebrtc.date
WHERE s.date <= CURRENT_DATE
{% if is_incremental() %}

AND s.date >= (SELECT MAX(date) FROM {{this}})

{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 
26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 
51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 
76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 
101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 
121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 
141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 
161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 
181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 
201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220,
 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 
 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 
 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 
 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 
 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 
 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 
 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 
 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 
 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 
 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 
 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 
 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460,
 461, 462, 463, 464, 465, 466, 467
)
SELECT *
FROM server_config_details