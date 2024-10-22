
with source as (
    select * from {{ source('mm_telemetry_prod', 'config_plugin') }}
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
        , coalesce(context_traits_installationid,  context_traits_installation_id) as installation_id
        , anonymous_id
        , context_ip as server_ip

        , allow_insecure_download_url
        , automatic_prepackaged_plugins
        , chimera_oauth_proxy_url
        , enable as enable_plugin
        , enable_alertmanager
        , enable_antivirus
        , enable_autolink
        , enable_aws_sns
        , enable_bitbucket
        , enable_classroom_autolink
        , enable_comgithubjespinorecommend
        , enable_comgithubmanlandmattermostplugingitlab
        , enable_comgithubmattermostplugincircleci
        , enable_comgithubmatterpollmatterpoll
        , enable_comgithubmoussetcmattermostplugindiceroller
        , enable_comgithubmoussetcmattermostplugingiphy
        , enable_comgithubphillipaherezamattermostplugindigitalocean
        , enable_commattermostagenda
        , enable_commattermostawssns
        , enable_commattermostconfluence
        , enable_commattermostcustomattributes
        , enable_commattermostmscalendar
        , enable_commattermostmsteamsmeetings
        , enable_commattermostnps
        , enable_commattermostpluginchannelexport
        , enable_commattermostpluginincidentmanagement
        , enable_commattermostpluginincidentresponse
        , enable_commattermostplugintodo
        , enable_commattermostwebex
        , enable_commattermostwelcomebot
        , enable_comnilsbrinkmannicebreaker
        , enable_com_classroom_aws_sns
        , enable_com_classroom_confluence
        , enable_com_classroom_custom_attributes
        , enable_com_classroom_mscalendar
        , enable_com_classroom_nps
        , enable_com_classroom_plugin_channel_export
        , enable_com_classroom_plugin_incident_management
        , enable_com_classroom_plugin_todo
        , enable_com_classroom_webex
        , enable_com_classroom_welcomebot
        , enable_com_github_jespino_recommend
        , enable_com_github_manland_classroom_plugin_gitlab
        , enable_com_github_manland_mattermost_plugin_gitlab
        , enable_com_github_mattermost_plugin_circleci
        , enable_com_github_matterpoll_matterpoll
        , enable_com_github_moussetc_classroom_plugin_giphy
        , enable_com_github_moussetc_mattermost_plugin_diceroller
        , enable_com_github_moussetc_mattermost_plugin_giphy
        , enable_com_github_phillipahereza_classroom_plugin_digitalocean
        , enable_com_github_phillipahereza_mattermost_plugin_digitalocean
        , enable_com_mattermost_agenda
        , enable_com_mattermost_apps
        , enable_com_mattermost_aws_sns
        , enable_com_mattermost_calls
        , enable_com_mattermost_confluence
        , enable_com_mattermost_custom_attributes
        , enable_com_mattermost_mscalendar
        , enable_com_mattermost_msteamsmeetings
        , enable_com_mattermost_msteams_sync
        , enable_com_mattermost_nps
        , enable_com_mattermost_plugin_channel_export
        , enable_com_mattermost_plugin_incident_management
        , enable_com_mattermost_plugin_incident_response
        , enable_com_mattermost_plugin_todo
        , enable_com_mattermost_webex
        , enable_com_mattermost_welcomebot
        , enable_com_nilsbrinkmann_icebreaker
        , enable_confluence
        , enable_custom_user_attributes
        , enable_focalboard
        , enable_github
        , enable_gitlab
        , enable_health_check
        , enable_jenkins
        , enable_jira
        , enable_jitsi
        , enable_marketplace
        , enable_mattermostautolink
        , enable_mattermostprofanityfilter
        , enable_mattermost_autolink
        , enable_mattermost_plugin_azure_devops
        , enable_mattermost_plugin_hackerone
        , enable_mattermost_plugin_servicenow
        , enable_mattermost_plugin_servicenow_virtual_agent
        , enable_mattermost_profanity_filter
        , enable_memes
        , enable_mscalendar
        , enable_nps
        , enable_nps_survey
        , enable_playbooks
        , enable_remote_marketplace
        , enable_ru_loop_plugin_embeds
        , enable_ru_loop_plugin_scheduler
        , enable_ru_loop_plugin_user_fields
        , enable_ru_loop_plugin_welcomebot
        , enable_set_default_theme
        , enable_skype4business
        , enable_skype_4_business
        , enable_todo
        , enable_uploads
        , enable_webex
        , enable_welcome_bot
        , enable_zoom
        , is_default_marketplace_url
        , require_pluginsignature
        , require_plugin_signature
        , signature_public_key_files
        , version_alertmanager
        , version_antivirus
        , version_autolink
        , version_aws_sns
        , version_bitbucket
        , version_comgithubjespinorecommend
        , version_comgithubmanlandmattermostplugingitlab
        , version_comgithubmattermostplugincircleci
        , version_comgithubmatterpollmatterpoll
        , version_comgithubmoussetcmattermostplugindiceroller
        , version_comgithubmoussetcmattermostplugingiphy
        , version_comgithubphillipaherezamattermostplugindigitalocean
        , version_commattermostagenda
        , version_commattermostawssns
        , version_commattermostconfluence
        , version_commattermostcustomattributes
        , version_commattermostmscalendar
        , version_commattermostmsteamsmeetings
        , version_commattermostnps
        , version_commattermostpluginchannelexport
        , version_commattermostpluginincidentmanagement
        , version_commattermostpluginincidentresponse
        , version_commattermostplugintodo
        , version_commattermostwebex
        , version_commattermostwelcomebot
        , version_comnilsbrinkmannicebreaker
        , version_com_github_jespino_recommend
        , version_com_github_manland_mattermost_plugin_gitlab
        , version_com_github_mattermost_plugin_circleci
        , version_com_github_matterpoll_matterpoll
        , version_com_github_moussetc_mattermost_plugin_diceroller
        , version_com_github_moussetc_mattermost_plugin_giphy
        , version_com_github_phillipahereza_mattermost_plugin_digitalocean
        , version_com_mattermost_agenda
        , version_com_mattermost_apps
        , version_com_mattermost_aws_sns
        , version_com_mattermost_calls
        , version_com_mattermost_confluence
        , version_com_mattermost_custom_attributes
        , version_com_mattermost_mscalendar
        , version_com_mattermost_msteamsmeetings
        , version_com_mattermost_msteams_sync
        , version_com_mattermost_nps
        , version_com_mattermost_plugin_channel_export
        , version_com_mattermost_plugin_incident_management
        , version_com_mattermost_plugin_incident_response
        , version_com_mattermost_plugin_todo
        , version_com_mattermost_webex
        , version_com_mattermost_welcomebot
        , version_com_nilsbrinkmann_icebreaker
        , version_custom_user_attributes
        , version_focalboard
        , version_github
        , version_gitlab
        , version_jenkins
        , version_jira
        , version_jitsi
        , version_mattermostautolink
        , version_mattermostprofanityfilter
        , version_mattermost_autolink
        , version_mattermost_plugin_azure_devops
        , version_mattermost_plugin_hackerone
        , version_mattermost_plugin_servicenow
        , version_mattermost_plugin_servicenow_virtual_agent
        , version_mattermost_profanity_filter
        , version_memes
        , version_nps
        , version_playbooks
        , version_set_default_theme
        , version_skype4business
        , version_skype_4_business
        , version_webex
        , version_welcome_bot
        , version_zoom
        
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
