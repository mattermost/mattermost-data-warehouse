with segment_oauth as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id

        , is_office365_enabled
        , is_google_enabled
        , is_gitlab_enabled
    from
        {{ ref('stg_mattermost2__oauth') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), rudderstack_oauth as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id

        , is_office365_enabled
        , is_google_enabled
        , is_gitlab_enabled
        , is_openid_enabled
        , is_openid_google_enabled
        , is_openid_gitlab_enabled
        , is_openid_office365_enabled
    from
        {{ ref('stg_mm_telemetry_prod__oauth') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), segment_plugin as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
        , {{ dbt_utils.star(ref('stg_mattermost2__plugin')) }}
    from
        {{ ref('stg_mattermost2__plugin') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), rudderstack_plugin as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
        , {{ dbt_utils.star(ref('stg_mm_telemetry_prod__plugin')) }}
    from
        {{ ref('stg_mm_telemetry_prod__plugin') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), segment_ldap as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
        , {{ dbt_utils.star(ref('stg_mattermost2__ldap')) }}
    from
        {{ ref('stg_mattermost2__ldap') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), rudderstack_ldap as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
        , {{ dbt_utils.star(ref('stg_mm_telemetry_prod__ldap')) }}
    from
        {{ ref('stg_mm_telemetry_prod__ldap') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), segment_saml as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
        , {{ dbt_utils.star(ref('stg_mattermost2__saml')) }}
    from
        {{ ref('stg_mattermost2__saml') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), rudderstack_saml as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
        , {{ dbt_utils.star(ref('stg_mm_telemetry_prod__saml')) }}
    from
        {{ ref('stg_mm_telemetry_prod__saml') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), segment_service as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
        , {{ dbt_utils.star(ref('stg_mattermost2__service')) }}
    from
        {{ ref('stg_mattermost2__service') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
), rudderstack_service as (
    select
        server_id
        , cast(timestamp as date) as server_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} as daily_server_id
        , {{ dbt_utils.star(ref('stg_mm_telemetry_prod__service')) }}
    from
        {{ ref('stg_mm_telemetry_prod__service') }}
    qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
)
select
    spine.daily_server_id
    , spine.server_id
    , spine.activity_date
    -- OAuth section
    , coalesce(r.is_office365_enabled, s.is_office365_enabled) as is_office365_enabled
    , coalesce(r.is_google_enabled, s.is_google_enabled) as is_google_enabled
    , coalesce(r.is_gitlab_enabled, s.is_gitlab_enabled) as is_gitlab_enabled
    , case when r.is_openid_enabled = true then true else false end as is_openid_enabled
    , case when r.is_openid_google_enabled = true then true else false end as is_openid_google_enabled
    , case when r.is_openid_gitlab_enabled = true then true else false end as is_openid_gitlab_enabled
    , case when r.is_openid_office365_enabled = true then true else false end as is_openid_office365_enabled
   
    -- Ldap section
    , coalesce(ls.connection_security_ldap, lr.connection_security_ldap)                             as connection_security_ldap
    , coalesce(ls.enable_ldap, lr.enable_ldap)                                                       as enable_ldap
    , coalesce(ls.enable_admin_filter, lr.enable_admin_filter)                                       as enable_admin_filter
    , coalesce(ls.enable_sync, lr.enable_sync)                                                       as enable_sync
    , coalesce(ls.isdefault_email_attribute_ldap, lr.isdefault_email_attribute_ldap)                 as isdefault_email_attribute_ldap
    , coalesce(ls.isdefault_first_name_attribute_ldap, lr.isdefault_first_name_attribute_ldap)       as isdefault_first_name_attribute_ldap
    , coalesce(ls.isdefault_group_display_name_attribute, lr.isdefault_group_display_name_attribute) as isdefault_group_display_name_attribute
    , coalesce(ls.isdefault_group_id_attribute, lr.isdefault_group_id_attribute)                     as isdefault_group_id_attribute
    , coalesce(ls.isdefault_id_attribute_ldap, lr.isdefault_id_attribute_ldap)                       as isdefault_id_attribute_ldap
    , coalesce(ls.isdefault_last_name_attribute_ldap, lr.isdefault_last_name_attribute_ldap)         as isdefault_last_name_attribute_ldap
    , coalesce(ls.isdefault_login_button_border_color_ldap, 
               lr.isdefault_login_button_border_color_ldap)                                          as isdefault_login_button_border_color_ldap
    , coalesce(ls.isdefault_login_button_color_ldap, lr.isdefault_login_button_color_ldap)           as isdefault_login_button_color_ldap
    , coalesce(ls.isdefault_login_button_text_color_ldap, lr.isdefault_login_button_text_color_ldap) as isdefault_login_button_text_color_ldap
    , coalesce(ls.isdefault_login_field_name, lr.isdefault_login_field_name)                         as isdefault_login_field_name
    , coalesce(ls.isdefault_login_id_attribute, lr.isdefault_login_id_attribute)                     as isdefault_login_id_attribute
    , coalesce(ls.isdefault_nickname_attribute_ldap, lr.isdefault_nickname_attribute_ldap)           as isdefault_nickname_attribute_ldap
    , coalesce(ls.isdefault_position_attribute_ldap, lr.isdefault_position_attribute_ldap)           as isdefault_position_attribute_ldap
    , coalesce(ls.isdefault_username_attribute_ldap, lr.isdefault_username_attribute_ldap)           as isdefault_username_attribute_ldap
    , coalesce(ls.isempty_admin_filter, lr.isempty_admin_filter)                                     as isempty_admin_filter
    , coalesce(ls.isempty_group_filter, lr.isempty_group_filter)                                     as isempty_group_filter
    , coalesce(ls.isempty_guest_filter, lr.isempty_guest_filter)                                     as isempty_guest_filter
    , coalesce(ls.isnotempty_picture_attribute, lr.isnotempty_picture_attribute)                     as isnotempty_picture_attribute
    , coalesce(lr.isnotempty_private_key, null)                                                      as isnotempty_private_key
    , coalesce(lr.isnotempty_public_certificate, null)                                               as isnotempty_public_certificate
    , coalesce(ls.max_page_size, lr.max_page_size)                                                   as max_page_size
    , coalesce(ls.query_timeout_ldap, lr.query_timeout_ldap)                                         as query_timeout_ldap
    , coalesce(ls.segment_dedupe_id_ldap, null)                                                      as segment_dedupe_id_ldap
    , coalesce(ls.skip_certificate_verification, lr.skip_certificate_verification)                   as skip_certificate_verification
    , coalesce(ls.sync_interval_minutes, lr.sync_interval_minutes)                                   as sync_interval_minutes
    
    -- Saml section
    , coalesce(ss.enable_saml, sr.enable_saml)                                                       as enable_saml
    , coalesce(ss.enable_admin_attribute, sr.enable_admin_attribute)                                 as enable_admin_attribute
    , coalesce(ss.enable_sync_with_ldap, sr.enable_sync_with_ldap)                                   as enable_sync_with_ldap
    , coalesce(ss.enable_sync_with_ldap_include_auth, sr.enable_sync_with_ldap_include_auth)         as enable_sync_with_ldap_include_auth
    , coalesce(ss.encrypt_saml, sr.encrypt_saml)                                                     as encrypt_saml
    , coalesce(sr.ignore_guests_ldap_sync, null)                                                     as ignore_guests_ldap_sync
    , coalesce(ss.isdefault_admin_attribute, sr.isdefault_admin_attribute)                           as isdefault_admin_attribute
    , coalesce(ss.isdefault_canonical_algorithm, sr.isdefault_canonical_algorithm)                   as isdefault_canonical_algorithm
    , coalesce(ss.isdefault_email_attribute_saml, sr.isdefault_email_attribute_saml)                 as isdefault_email_attribute_saml
    , coalesce(ss.isdefault_first_name_attribute_saml, sr.isdefault_first_name_attribute_saml)       as isdefault_first_name_attribute_saml
    , coalesce(ss.isdefault_guest_attribute, sr.isdefault_guest_attribute)                           as isdefault_guest_attribute
    , coalesce(ss.isdefault_id_attribute_saml, sr.isdefault_id_attribute_saml)                       as isdefault_id_attribute_saml
    , coalesce(ss.isdefault_last_name_attribute_saml, sr.isdefault_last_name_attribute_saml)         as isdefault_last_name_attribute_saml
    , coalesce(ss.isdefault_locale_attribute, sr.isdefault_locale_attribute)                         as isdefault_locale_attribute
    , coalesce(ss.isdefault_login_button_border_color_saml, 
               sr.isdefault_login_button_border_color_saml)                                          as isdefault_login_button_border_color_saml
    , coalesce(ss.isdefault_login_button_color_saml, sr.isdefault_login_button_color_saml)           as isdefault_login_button_color_saml
    , coalesce(ss.isdefault_login_button_text, sr.isdefault_login_button_text)                       as isdefault_login_button_text
    , coalesce(ss.isdefault_login_button_text_color_saml, sr.isdefault_login_button_text_color_saml) as isdefault_login_button_text_color_saml
    , coalesce(ss.isdefault_nickname_attribute_saml, sr.isdefault_nickname_attribute_saml)           as isdefault_nickname_attribute_saml
    , coalesce(ss.isdefault_position_attribute_saml, sr.isdefault_position_attribute_saml)           as isdefault_position_attribute_saml
    , coalesce(ss.isdefault_scoping_idp_name, sr.isdefault_scoping_idp_name)                         as isdefault_scoping_idp_name
    , coalesce(ss.isdefault_scoping_idp_provider_id, sr.isdefault_scoping_idp_provider_id)           as isdefault_scoping_idp_provider_id
    , coalesce(ss.isdefault_signature_algorithm, sr.isdefault_signature_algorithm)                   as isdefault_signature_algorithm
    , coalesce(ss.isdefault_username_attribute_saml, sr.isdefault_username_attribute_saml)           as isdefault_username_attribute_saml
    , coalesce(ss.sign_request, sr.sign_request)                                                     as sign_request
    , coalesce(ss.verify_saml, sr.verify_saml)                                                       as verify_saml           
        
    -- Plugin section
    , coalesce(ps.allow_insecure_download_url, pr.allow_insecure_download_url)                       as allow_insecure_download_url
    , coalesce(ps.automatic_prepackaged_plugins, pr.automatic_prepackaged_plugins)                   as automatic_prepackaged_plugins
    , coalesce(pr.chimera_oauth_proxy_url, null)                                                     as chimera_oauth_proxy_url
    , coalesce(ps.is_default_marketplace_url, pr.is_default_marketplace_url)                         as is_default_marketplace_url
    , coalesce(ps.require_plugin_signature, pr.require_pluginsignature)                              as require_plugin_signature
    , coalesce(ps.signature_public_key_files, pr.signature_public_key_files)                         as signature_public_key_files
    
    -- PLUGINS ENABLED FLAGS
    , coalesce(ps.enable_plugin, pr.enable_plugin)                                                   as enable_plugin
    , coalesce(pr.enable_alertmanager, null)                                                         as enable_alertmanager
    , coalesce(ps.enable_antivirus, pr.enable_antivirus)                                             as enable_antivirus
    , coalesce(ps.enable_autolink, 
               ps.enable_autolink, 
               pr.enable_classroom_autolink, 
               pr.enable_mattermostautolink, 
               pr.enable_mattermost_autolink)                                                        as enable_autolink
    , coalesce(ps.enable_aws_sns, 
               ps.enable_aws_sns, 
               pr.enable_com_classroom_aws_sns, 
               pr.enable_commattermostawssns, 
               pr.enable_com_mattermost_aws_sns)                                                     as enable_aws_sns
    , coalesce(pr.enable_bitbucket,null)                                                             as enable_bitbucket
    , coalesce(ps.enable_confluence, 
               pr.enable_confluence, 
               pr.enable_commattermostconfluence, 
               pr.enable_com_classroom_confluence,
               pr.enable_com_mattermost_confluence)                                                  as enable_confluence
    , coalesce(ps.enable_custom_user_attributes, 
               pr.enable_commattermostcustomattributes, 
               pr.enable_custom_user_attributes, 
               pr.enable_com_mattermost_custom_attributes)                                           as enable_custom_user_attributes
    , coalesce(pr.enable_focalboard,null)                                                            as enable_focalboard
    , coalesce(ps.enable_github, pr.enable_github)                                                   as enable_github
    , coalesce(ps.enable_gitlab, 
               pr.enable_gitlab, 
               pr.enable_comgithubmanlandmattermostplugingitlab, 
               pr.enable_com_github_manland_classroom_plugin_gitlab, 
               pr.enable_com_github_manland_mattermost_plugin_gitlab)                                as enable_gitlab
    , coalesce(ps.enable_health_check, pr.enable_health_check)                                       as enable_health_check
    , coalesce(ps.enable_jenkins, pr.enable_jenkins)                                                 as enable_jenkins
    , coalesce(ps.enable_jira, pr.enable_jira)                                                       as enable_jira
    , coalesce(ps.enable_jitsi, pr.enable_jitsi)                                                     as enable_jitsi
    , coalesce(ps.enable_marketplace, pr.enable_marketplace)                                         as enable_marketplace
    , coalesce(pr.enable_mattermostprofanityfilter, pr.enable_mattermost_profanity_filter)           as enable_mattermostprofanityfilter
    , coalesce(pr.enable_mattermost_plugin_azure_devops, null)                                       as enable_mattermost_plugin_azure_devops
    , coalesce(pr.enable_mattermost_plugin_hackerone, null)                                          as enable_mattermost_plugin_hackerone
    , coalesce(pr.enable_mattermost_plugin_servicenow, null)                                         as enable_mattermost_plugin_servicenow
    , coalesce(pr.enable_mattermost_plugin_servicenow_virtual_agent, null)                           as enable_mattermost_plugin_servicenow_virtual_agent
    , coalesce(pr.enable_memes, null)                                                                as enable_memes
    , coalesce(ps.enable_mscalendar, 
               pr.enable_mscalendar,
               pr.enable_commattermostmscalendar, 
               pr.enable_com_classroom_mscalendar, 
               pr.enable_com_mattermost_mscalendar)                                                  as enable_mscalendar
    , coalesce(ps.enable_nps, pr.enable_nps, 
               pr.enable_commattermostnps, 
               pr.enable_com_classroom_nps,
               pr.enable_com_mattermost_nps)                                                         as enable_nps
    , coalesce(ps.enable_nps_survey, pr.enable_nps_survey)                                           as enable_nps_survey
    , coalesce(pr.enable_playbooks, null)                                                            as enable_playbooks
    , coalesce(ps.enable_remote_marketplace, null)                                                   as enable_remote_marketplace
    , coalesce(pr.enable_set_default_theme, null)                                                    as enable_set_default_theme
    , coalesce(ps.enable_skype4business, pr.enable_skype4business, pr.enable_skype_4_business)       as enable_skype4business
    , coalesce(ps.enable_todo, 
               pr.enable_todo, 
               pr.enable_commattermostplugintodo,
               pr.enable_com_classroom_plugin_todo,
               pr.enable_com_mattermost_plugin_todo)                                                 as enable_todo
    , coalesce(ps.enable_uploads, pr.enable_uploads)                                                 as enable_uploads
    , coalesce(ps.enable_webex, 
               pr.enable_webex, 
               pr.enable_commattermostwebex,
               pr.enable_com_classroom_webex,
               pr.enable_com_mattermost_webex)                                                       as enable_webex
    , coalesce(ps.enable_welcome_bot, 
               pr.enable_welcome_bot, 
               pr.enable_commattermostwelcomebot, 
               pr.enable_com_classroom_welcomebot,
               pr.enable_com_mattermost_welcomebot)                                                  as enable_welcome_bot
    , coalesce(ps.enable_zoom, pr.enable_zoom)                                                       as enable_zoom
    , coalesce(pr.enable_comgithubmoussetcmattermostplugingiphy, 
               pr.enable_com_github_moussetc_classroom_plugin_giphy,
               pr.enable_com_github_moussetc_mattermost_plugin_giphy)                                as enable_giphy
    , coalesce(pr.enable_comgithubphillipaherezamattermostplugindigitalocean,
               pr.enable_com_github_phillipahereza_classroom_plugin_digitalocean,
               pr.enable_com_github_phillipahereza_mattermost_plugin_digitalocean)                   as enable_digital_ocean
    , coalesce(pr.enable_commattermostagenda, pr.enable_com_mattermost_agenda)                       as enable_agenda
    , coalesce(pr.enable_com_mattermost_apps,null)                                                   as enable_mattermost_apps
    , coalesce(pr.enable_com_mattermost_calls, null)                                                 as enable_calls
    , coalesce(pr.enable_commattermostpluginincidentmanagement, 
               pr.enable_com_mattermost_plugin_incident_management)                                  as enable_incident_management
    , coalesce(pr.enable_commattermostpluginincidentresponse, 
               pr.enable_com_mattermost_plugin_incident_response)                                    as enable_incident_response
    , coalesce(pr.enable_comgithubmatterpollmatterpoll, 
               pr.enable_com_github_matterpoll_matterpoll)                                           as enable_matterpoll
    , coalesce(pr.enable_com_github_moussetc_mattermost_plugin_diceroller, 
               pr.enable_comgithubmoussetcmattermostplugindiceroller)                                as enable_diceroller
    , coalesce(pr.enable_comgithubjespinorecommend, pr.enable_com_github_jespino_recommend)          as enable_comgithubjespinorecommend
    , coalesce(pr.enable_commattermostmsteamsmeetings, 
               pr.enable_com_mattermost_msteamsmeetings)                                             as enable_msteams_meetings
    , coalesce(pr.enable_commattermostpluginchannelexport, 
               pr.enable_com_mattermost_plugin_channel_export)                                       as enable_commattermostpluginchannelexport 
    , coalesce(pr.enable_comnilsbrinkmannicebreaker, pr.enable_com_nilsbrinkmann_icebreaker)         as enable_comnilsbrinkmannicebreaker
    , coalesce(pr.enable_com_github_mattermost_plugin_circleci, 
               pr.enable_comgithubmattermostplugincircleci)                                          as enable_circleci
    
    -- VERSIONS 
    , coalesce(pr.version_alertmanager, null)                                                        as version_alertmanager
    , coalesce(ps.version_antivirus, pr.version_antivirus)                                           as version_antivirus
    , coalesce(ps.version_autolink, pr.version_autolink, 
               pr.version_mattermostautolink, 
               pr.version_mattermost_autolink)                                                       as version_autolink
    , coalesce(ps.version_aws_sns, 
               pr.version_aws_sns, 
               pr.version_commattermostawssns, 
               pr.version_com_mattermost_aws_sns)                                                    as version_aws_sns
    , coalesce(ps.version_custom_user_attributes, 
               pr.version_custom_user_attributes, 
               pr.version_commattermostcustomattributes, 
               pr.version_com_mattermost_custom_attributes)                                          as version_custom_user_attributes
    , coalesce(ps.version_github, pr.version_github)                                                 as version_github
    , coalesce(ps.version_gitlab, 
               pr.version_gitlab, 
               pr.version_comgithubmanlandmattermostplugingitlab, 
               pr.version_com_github_manland_mattermost_plugin_gitlab)                               as version_gitlab
    , coalesce(ps.version_jenkins, pr.version_jenkins)                                               as version_jenkins
    , coalesce(ps.version_jira, pr.version_jira)                                                     as version_jira
    , coalesce(ps.version_nps, 
               pr.version_nps, 
               pr.version_commattermostnps, 
               pr.version_com_mattermost_nps)                                                        as version_nps
    , coalesce(ps.version_webex, pr.version_webex, 
               pr.version_commattermostwebex, 
               pr.version_com_mattermost_webex)                                                      as version_webex
    , coalesce(ps.version_welcome_bot, 
               pr.version_welcome_bot, 
               pr.version_commattermostwelcomebot, 
               pr.version_com_mattermost_welcomebot)                                                 as version_welcome_bot
    , coalesce(ps.version_zoom, pr.version_zoom)                                                     as version_zoom
    , coalesce(pr.version_comgithubmoussetcmattermostplugingiphy, 
               pr.version_com_github_moussetc_mattermost_plugin_giphy)                               as version_giphy
    , coalesce(pr.version_comgithubphillipaherezamattermostplugindigitalocean, 
               pr.version_com_github_phillipahereza_mattermost_plugin_digitalocean)                  as version_digital_ocean
    , coalesce(pr.version_commattermostconfluence, pr.version_com_mattermost_confluence)             as version_confluence
    , coalesce(pr.version_commattermostmscalendar, pr.version_com_mattermost_mscalendar)             as version_mscalendar
    , coalesce(pr.version_commattermostpluginincidentmanagement, 
               pr.version_com_mattermost_plugin_incident_management)                                 as version_incident_management
    , coalesce(pr.version_commattermostpluginincidentresponse, 
               pr.version_com_mattermost_plugin_incident_response)                                   as version_incident_response
    , coalesce(pr.version_commattermostplugintodo, pr.version_com_mattermost_plugin_todo)            as version_todo
    , coalesce(pr.version_memes, null)                                                               as version_memes
    , coalesce(pr.version_jitsi, null)                                                               as version_jitsi
    , coalesce(pr.version_skype4business, pr.version_skype_4_business)                               as version_skype4business
    , coalesce(pr.version_mattermostprofanityfilter, pr.version_mattermost_profanity_filter)         as version_mattermostprofanityfilter
    , coalesce(pr.version_comgithubmatterpollmatterpoll, 
               pr.version_com_github_matterpoll_matterpoll)                                          as version_materpoll
    , coalesce(pr.version_comgithubjespinorecommend, pr.version_com_github_jespino_recommend)        as version_comgithubjespinorecommend
    , coalesce(pr.version_commattermostagenda, pr.version_com_mattermost_agenda)                     as version_agenda        
    , coalesce(pr.version_commattermostmsteamsmeetings, pr.version_com_mattermost_msteamsmeetings)   as version_msteams_meetings
    , coalesce(pr.version_commattermostpluginchannelexport, 
               pr.version_com_mattermost_plugin_channel_export)                                      as version_commattermostpluginchannelexport
    , coalesce(pr.version_comnilsbrinkmannicebreaker, pr.version_com_nilsbrinkmann_icebreaker)       as version_comnilsbrinkmannicebreaker
    , coalesce(pr.version_com_mattermost_apps, null)                                                 as version_mattermost_apps
    , coalesce(pr.version_com_github_mattermost_plugin_circleci, 
               pr.version_comgithubmattermostplugincircleci)                                         as version_circleci
    , coalesce(pr.version_comgithubmoussetcmattermostplugindiceroller, 
               pr.version_com_github_moussetc_mattermost_plugin_diceroller)                          as version_diceroller
    , coalesce(pr.version_focalboard, null)                                                          as version_focalboard

    , coalesce(pr.context_traits_installationid, pr.context_traits_installation_id)                  as installation_id_plugin
    -- Service section
    , coalesce(vs.allow_cookies_for_subdomains, vr.allow_cookies_for_subdomains)                     as allow_cookies_for_subdomains
    , coalesce(vs.allow_edit_post_service, vr.allow_edit_post_service)                               as allow_edit_post_service
    , coalesce(vr.allow_persistent_notifications, null)                                              as allow_persistent_notifications
    , coalesce(vr.allow_persistent_notifications_for_guests, null)                                   as allow_persistent_notifications_for_guests                                       
    , coalesce(vr.allow_synced_drafts, null)                                                         as allow_synced_drafts
    , coalesce(vs.close_unused_direct_messages, vr.close_unused_direct_messages)                     as close_unused_direct_messages
    , coalesce(vs.cluster_log_timeout_milliseconds, vr.cluster_log_timeout_milliseconds)             as cluster_log_timeout_milliseconds
    , coalesce(vr.collapsed_threads, null)                                                           as collapsed_threads
    , coalesce(vs.connection_security_service, vr.connection_security_service)                       as connection_security_service
    , coalesce(vs.cors_allow_credentials, vr.cors_allow_credentials)                                 as cors_allow_credentials
    , coalesce(vs.cors_debug, vr.cors_debug)                                                         as cors_debug
    , coalesce(vs.custom_service_terms_enabled, null)                                                as custom_service_terms_enabled
    , coalesce(vr.custom_cert_header, null)                                                          as custom_cert_header
    , coalesce(vr.default_team_name, null)                                                           as default_team_name
    , coalesce(vr.developer_flags, null)                                                             as developer_flags
    , coalesce(vs.disable_bots_when_owner_is_deactivated, 
               vr.disable_bots_when_owner_is_deactivated)                                            as disable_bots_when_owner_is_deactivated
    , coalesce(vs.disable_legacy_mfa, vr.disable_legacy_mfa)                                         as disable_legacy_mfa
    , coalesce(vs.enable_apiv3, null)                                                                as enable_apiv3
    , coalesce(vr.enable_api_channel_deletion, null)                                                 as enable_api_channel_deletion
    , coalesce(vr.enable_api_post_deletion, null)                                                    as enable_api_post_deletion
    , coalesce(vs.enable_api_team_deletion, vr.enable_api_team_deletion)                             as enable_api_team_deletion
    , coalesce(vr.enable_api_trigger_admin_notification, null)                                       as enable_api_trigger_admin_notification
    , coalesce(vr.enable_api_user_deletion, null)                                                    as enable_api_user_deletion
    , coalesce(vs.enable_bot_account_creation, vr.enable_bot_account_creation)                       as enable_bot_account_creation
    , coalesce(vs.enable_channel_viewed_messages_service, 
               vr.enable_channel_viewed_messages_service)                                            as enable_channel_viewed_messages_service
    , coalesce(vs.enable_commands_service, vr.enable_commands_service)                               as enable_commands_service
    , coalesce(vs.enable_custom_emoji_service, vr.enable_custom_emoji_service)                       as enable_custom_emoji_service
    , coalesce(vs.enable_developer_service, vr.enable_developer_service)                             as enable_developer_service
    , coalesce(vs.enable_email_invitations, vr.enable_email_invitations)                             as enable_email_invitations
    , coalesce(vs.enable_emoji_picker_service, vr.enable_emoji_picker_service)                       as enable_emoji_picker_service
    , coalesce(vr.enable_file_search, null)                                                          as enable_file_search
    , coalesce(vs.enable_gif_picker, vr.enable_gif_picker)                                           as enable_gif_picker
    , coalesce(vs.enable_incoming_webhooks_service, vr.enable_incoming_webhooks_service)             as enable_incoming_webhooks_service
    , coalesce(vs.enable_insecure_outgoing_connections_service, 
               vr.enable_insecure_outgoing_connections_service)                                      as enable_insecure_outgoing_connections_service
    , coalesce(vs.enable_latex, vr.enable_latex, vr.enable_latez)                                    as enable_latex
    , coalesce(vr.enable_legacy_sidebar, null)                                                       as enable_legacy_sidebar
    , coalesce(vr.enable_link_previews, null)                                                        as enable_link_previews
    , coalesce(vs.enable_local_mode, vr.enable_local_mode)                                           as enable_local_mode
    , coalesce(vs.enable_multifactor_authentication_service, 
               vr.elable_multifactor_authentication,
               vr.enable_multifactor_authentication_service)                                         as enable_multifactor_authentication_service
    , coalesce(vs.enable_oauth_service_provider_service, 
               vr.enable_oauth_service_provider_service)                                             as enable_oauth_service_provider_service
    , coalesce(vr.enable_onboarding_flow, null)                                                      as enable_onboarding_flow
    , coalesce(vs.enable_only_admin_integrations_service, 
               vr.enable_only_admin_integrations_service)                                            as enable_only_admin_integrations_service
    , coalesce(vs.enable_opentracing, vr.enable_opentracing)                                         as enable_opentracing
    , coalesce(vr.enable_outgoing_oauth_connections, null)                                           as enable_outgoing_oauth_connections
    , coalesce(vs.enable_outgoing_webhooks, vr.enable_outgoing_webhooks)                             as enable_outgoing_webhooks
    , coalesce(vr.enable_permalink_previews, null)                                                   as enable_permalink_previews
    , coalesce(vs.enable_post_icon_override, vr.enable_post_icon_override)                           as enable_post_icon_override
    , coalesce(vs.enable_post_search, vr.enable_post_search)                                         as enable_post_search
    , coalesce(vs.enable_post_username_override, vr.enable_post_username_override)                   as enable_post_username_override
    , coalesce(vs.enable_preview_features, vr.enable_preview_features)                               as enable_preview_features
    , coalesce(vs.enable_security_fix_alert, vr.enable_security_fix_alert)                           as enable_security_fix_alert
    , coalesce(vs.enable_svgs, vr.enable_svgs)                                                       as enable_svgs
    , coalesce(vs.enable_testing, vr.enable_testing)                                                 as enable_testing
    , coalesce(vs.enable_tutorial, vr.enable_tutorial)                                               as enable_tutorial
    , coalesce(vs.enable_user_access_tokens, vr.enable_user_access_tokens)                           as enable_user_access_tokens
    , coalesce(vs.enable_user_statuses, vr.enable_user_statuses)                                     as enable_user_statuses
    , coalesce(vs.enable_user_typing_messages, vr.enable_user_typing_messages)                       as enable_user_typing_messages
    , coalesce(vs.enforce_multifactor_authentication_service,
               vr.enforce_multifactor_authentication_service)                                        as enforce_multifactor_authentication_service
    , coalesce(vs.experimental_channel_organization, vr.experimental_channel_organization)           as experimental_channel_organization
    , coalesce(vs.experimental_channel_sidebar_organization, 
               vr.experimental_channel_sidebar_organization)                                         as experimental_channel_sidebar_organization
    , coalesce(vr.experimental_data_prefetch, null)                                                  as experimental_data_prefetch
    , coalesce(vs.experimental_enable_authentication_transfer, 
               vr.experimental_enable_authentication_transfer)                                       as experimental_enable_authentication_transfer
    , coalesce(vs.experimental_enable_default_channel_leave_join_messages, 
               vr.experimental_enable_default_channel_leave_join_messages)                           as experimental_enable_default_channel_leave_join_messages
    , coalesce(vs.experimental_enable_hardened_mode, 
               vr.experimental_enable_hardened_mode)                                                 as experimental_enable_hardened_mode
    , coalesce(vs.experimental_group_unread_channels, 
               vr.experimental_group_unread_channels)                                                as experimental_group_unread_channels
    , coalesce(vs.experimental_ldap_group_sync, null)                                                as experimental_ldap_group_sync
    , coalesce(vs.experimental_limit_client_config, null)                                            as experimental_limit_client_config
    , coalesce(vs.experimental_strict_csrf_enforcement, 
               vr.experimental_strict_csrf_enforcement)                                              as experimental_strict_csrf_enforcement
    , coalesce(vs.extend_session_length_with_activity, vr.extend_session_length_with_activity)       as extend_session_length_with_activity
    , coalesce(vs.forward_80_to_443, vr.forward_80_to_443)                                           as forward_80_to_443
    , coalesce(vs.gfycat_api_key, vr.gfycat_api_key)                                                 as gfycat_api_key
    , coalesce(vs.gfycat_api_secret, vr.gfycat_api_secret)                                           as gfycat_api_secret
    , coalesce(vs.isdefault_allowed_untrusted_internal_connections,
               vs.isdefault_allowed_untrusted_inteznal_connections, 
               vr.isdefault_allowed_untrusted_internal_connections)                                  as isdefault_allowed_untrusted_internal_connections
    , coalesce(vs.isdefault_allow_cors_from, vr.isdefault_allow_cors_from)                           as isdefault_allow_cors_from 
    , coalesce(vs.isdefault_cors_exposed_headers, vr.isdefault_cors_exposed_headers)                 as isdefault_cors_exposed_headers
    , coalesce(vs.isdefault_google_developer_key, vr.isdefault_google_developer_key)                 as isdefault_google_developer_key
    , coalesce(vs.isdefault_idle_timeout, vr.isdefault_idle_timeout)                                 as isdefault_idle_timeout   
    , coalesce(vs.isdefault_image_proxy_options, null)                                               as isdefault_image_proxy_options
    , coalesce(vs.isdefault_image_proxy_type, null)                                                  as isdefault_image_proxy_type
    , coalesce(vs.isdefault_image_proxy_url, null)                                                   as isdefault_image_proxy_url
    , coalesce(vs.isdefault_read_timeout, vr.isdefault_read_timeout)                                 as isdefault_read_timeout
    , coalesce(vs.isdefault_site_url, vr.isdefault_site_url)                                         as isdefault_site_url
    , coalesce(vs.isdefault_tls_cert_file, vr.isdefault_tls_cert_file)                               as isdefault_tls_cert_file
    , coalesce(vs.isdefault_tls_key_file, vr.isdefault_tls_key_file)                                 as isdefault_tls_key_file
    , coalesce(vs.isdefault_write_timeout, vr.isdefault_write_timeout)                               as isdefault_write_timeout
    , coalesce(vr.limit_load_search_result, null)                                                    as limit_load_search_result
    , coalesce(vr.login_with_certificate, null)                                                      as login_with_certificate
    , coalesce(vr.managed_resource_paths, null)                                                      as managed_resource_paths
    , coalesce(vs.maximum_login_attempts, vr.maximum_login_attempts)                                 as maximum_login_attempts
    , coalesce(vr.maximum_payload_size, null)                                                        as maximum_payload_size
    , coalesce(vr.maximum_url_length, null)                                                          as maximum_url_length
    , coalesce(vs.minimum_hashtag_length, vr.minimum_hashtag_length)                                 as minimum_hashtag_length
    , coalesce(vr.outgoing_integrations_requests_timeout, null)                                      as outgoing_integrations_requests_timeout
    , coalesce(vr.persistent_notification_interval_minutes, null)                                    as persistent_notification_interval_minutes
    , coalesce(vr.persistent_notification_max_count, null)                                           as persistent_notification_max_count
    , coalesce(vr.persistent_notification_max_recipients, null)                                      as persistent_notification_max_recipients
    , coalesce(vs.post_edit_time_limit, vr.post_edit_time_limit)                                     as post_edit_time_limit
    , coalesce(vr.post_priority, null)                                                               as post_priority
    , coalesce(vr.refresh_post_stats_run_time, null)                                                 as refresh_post_stats_run_time
    , coalesce(vs.restrict_custom_emoji_creation, vr.restrict_custom_emoji_creation)                 as restrict_custom_emoji_creation
    , coalesce(vr.restrict_link_previews, null)                                                      as restrict_link_previews
    , coalesce(vs.restrict_post_delete, vr.restrict_post_delete)                                     as restrict_post_delete
    , coalesce(vr.self_hosted_expansion, null)                                                       as self_hosted_expansion
    , coalesce(vr.self_hosted_purchase, null)                                                        as self_hosted_purchase 
    , coalesce(vs.session_cache_in_minutes, vr.session_cache_in_minutes)                             as session_cache_in_minutes
    , coalesce(vs.session_idle_timeout_in_minutes, vr.session_idle_timeout_in_minutes)               as session_idle_timeout_in_minutes
    , coalesce(vs.session_length_mobile_in_days, vr.session_length_mobile_in_days)                   as session_length_mobile_in_days
    , coalesce(vr.session_length_mobile_in_hours, null)                                              as session_length_mobile_in_hours
    , coalesce(vs.session_length_sso_in_days, vr.session_length_sso_in_days)                         as session_length_sso_in_days
    , coalesce(vr.session_length_sso_in_hours, null)                                                 as session_length_sso_in_hours
    , coalesce(vs.session_length_web_in_days, vr.session_length_web_in_days)                         as session_length_web_in_days
    , coalesce(vr.session_length_web_in_hours, null)                                                 as session_length_web_in_hours
    , coalesce(vr.skip_login_page, null)                                                             as skip_login_page
    , coalesce(vr.terminate_sessions_on_password_change, null)                                       as terminate_sessions_on_password_change
    , coalesce(vr.thread_auto_follow, null)                                                          as thread_auto_follow
    , coalesce(vs.time_between_user_typing_updates_milliseconds, 
               vr.time_between_user_typing_updates_milliseconds)                                     as time_between_user_typing_updates_milliseconds
    , coalesce(vs.tls_strict_transport, vr.tls_strict_transport)                                     as tls_strict_transport
    , coalesce(vs.uses_letsencrypt, vr.uses_letsencrypt)                                             as uses_letsencrypt
    , coalesce(vs.websocket_url, vr.websocket_url)                                                   as websocket_url
    , coalesce(vs.web_server_mode, vr.web_server_mode)                                               as web_server_mode           
    
    , coalesce(vr.context_traits_installationid, vr.context_traits_installation_id)                  as installation_id_service
    -- Metadata
    , s.server_id is not null or ls.server_id is not null or ss.server_id is not null or ps.server_id is not null or vs.server_id is not null as has_segment_telemetry_data
    , r.server_id is not null or lr.server_id is not null or sr.server_id is not null or pr.server_id is not null or vr.server_id is not null as has_rudderstack_telemetry_data
from
    {{ ref('int_server_active_days_spined') }} spine
    left join segment_oauth s on spine.daily_server_id = s.daily_server_id
    left join rudderstack_oauth r on spine.daily_server_id = r.daily_server_id
    left join segment_ldap ls on spine.daily_server_id = ls.daily_server_id
    left join rudderstack_ldap lr on spine.daily_server_id = lr.daily_server_id
    left join segment_saml ss on spine.daily_server_id = ss.daily_server_id
    left join rudderstack_saml sr on spine.daily_server_id = sr.daily_server_id
    left join segment_plugin ps on spine.daily_server_id = ps.daily_server_id
    left join rudderstack_plugin pr on spine.daily_server_id = pr.daily_server_id
    left join segment_service vs on spine.daily_server_id = vs.daily_server_id
    left join rudderstack_service vr on spine.daily_server_id = vr.daily_server_id