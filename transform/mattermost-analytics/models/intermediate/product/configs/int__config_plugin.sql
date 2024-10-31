select event_id
    , event_table
    , event_name
    , server_id
    , received_at
    , timestamp

    , null as installation_id
    , null as anonymous_id
    , null as server_ip

    , allow_insecure_download_url
    , automatic_prepackaged_plugins
    , null as chimera_oauth_proxy_url
    , enable_plugin
    , null as enable_alertmanager
    , enable_antivirus
    , enable_autolink
    , enable_aws_sns
    , null as enable_bitbucket
    , null as enable_channel_export
    , null as enable_circleci
    , enable_confluence
    , enable_custom_user_attributes
    , null as enable_diceroller
    , null as enable_digitalocean
    , null as enable_focalboard
    , null as enable_giphy
    , enable_github
    , enable_gitlab
    , enable_health_check
    , null as enable_icebreaker
    , null as enable_incident_management
    , null as enable_incident_response
    , enable_jenkins
    , enable_jespino_recommend
    , enable_jira
    , enable_jitsi
    , enable_marketplace
    , null as enable_materpoll
    , null as enable_mattermost_agenda
    , null as enable_mattermost_apps
    , null as enable_mattermost_azure_devops
    , null as enable_mattermost_calls
    , null as enable_mattermost_hackerone
    , null as enable_mattermost_msteams_meetings
    , null as enable_mattermost_msteams_sync
    , null as enable_mattermost_profanity_filter
    , null as enable_mattermost_servicenow
    , null as enable_mattermost_servicenow_virtual_agent
    , null as enable_memes
    , enable_mscalendar
    , enable_nps
    , enable_nps_survey
    , null as enable_playbooks
    , enable_remote_marketplace
    , null as enable_ru_loop_plugin_embeds
    , null as enable_ru_loop_plugin_scheduler
    , null as enable_ru_loop_plugin_user_fields
    , null as enable_set_default_theme
    , enable_skype4business
    , enable_todo
    , enable_uploads
    , enable_webex
    , enable_welcome_bot
    , enable_zoom
    , is_default_marketplace_url
    , require_plugin_signature
    , signature_public_key_files
    , null as version_alertmanager
    , version_antivirus
    , version_autolink
    , version_aws_sns
    , null as version_bitbucket
    , null as version_channel_export
    , null as version_circleci
    , null as version_confluence
    , version_custom_user_attributes
    , null as version_diceroller
    , null as version_digitalocean
    , null as version_giphy
    , version_github
    , version_gitlab
    , null as version_icebreaker
    , null as version_incident_management
    , null as version_incident_response
    , version_jenkins
    , null as version_jespino_recommend
    , version_jira
    , null as version_jitsi
    , null as version_materpoll
    , null as version_mattermost_agenda
    , null as version_mattermost_apps
    , null as version_mattermost_azure_devops
    , null as version_mattermost_calls
    , null as version_mattermost_hackerone
    , null as version_mattermost_msteams_meetings
    , null as version_mattermost_msteams_sync
    , null as version_mattermost_profanity_filter
    , null as version_mattermost_servicenow
    , null as version_mattermost_servicenow_virtual_agent
    , null as version_memes
    , null as version_mscalendar
    , version_nps
    , null as version_playbooks
    , null as version_set_default_theme
    , null as version_todo
    , version_webex
    , version_welcome_bot
    , version_zoom

    -- Metadata from Segment
    , context_library_name
    , context_library_version
    , sent_at
    , original_timestamp
from {{ ref('int_mattermost2__config_saml') }}
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

    , allow_insecure_download_url
    , automatic_prepackaged_plugins
    , chimera_oauth_proxy_url
    , enable_plugin
    , enable_alertmanager
    , enable_antivirus
    , coalesce(enable_autolink, 
               enable_classroom_autolink,
               enable_mattermostautolink,
               enable_mattermost_autolink)                                       as enable_autolink
    , coalesce(enable_aws_sns, 
               enable_commattermostawssns,
               enable_com_mattermost_aws_sns,
               enable_com_classroom_aws_sns)                                     as enable_aws_sns
    , enable_bitbucket
    , coalesce(enable_commattermostpluginchannelexport, 
               enable_com_classroom_plugin_channel_export,
               enable_com_mattermost_plugin_channel_export)                      as enable_channel_export
    , coalesce(enable_comgithubmattermostplugincircleci,
               enable_com_github_mattermost_plugin_circleci)                     as enable_circleci
    , coalesce(enable_confluence, 
               enable_commattermostconfluence, 
               enable_com_classroom_confluence,
               enable_com_mattermost_confluence)                                 as enable_confluence
    , coalesce(enable_custom_user_attributes, 
               enable_commattermostcustomattributes,
               enable_com_mattermost_custom_attributes,
               enable_com_classroom_custom_attributes)                           as enable_custom_user_attributes
    , coalesce(enable_comgithubmoussetcmattermostplugindiceroller,
               enable_com_github_moussetc_mattermost_plugin_diceroller)          as enable_diceroller
    , coalesce(enable_comgithubphillipaherezamattermostplugindigitalocean,
               enable_com_github_phillipahereza_classroom_plugin_digitalocean,
               enable_com_github_phillipahereza_mattermost_plugin_digitalocean)  as enable_digitalocean
    , enable_focalboard
    , coalesce(enable_comgithubmoussetcmattermostplugingiphy,
               enable_com_github_moussetc_mattermost_plugin_giphy,
               enable_com_github_moussetc_classroom_plugin_giphy)                as enable_giphy
    , enable_github
    , coalesce(enable_gitlab, 
               enable_comgithubmanlandmattermostplugingitlab, 
               enable_com_github_manland_classroom_plugin_gitlab,
               enable_com_github_manland_mattermost_plugin_gitlab)               as enable_gitlab
    , enable_health_check
    , coalesce(enable_comnilsbrinkmannicebreaker,
               enable_com_nilsbrinkmann_icebreaker)                              as enable_icebreaker
    , coalesce(enable_commattermostpluginincidentmanagement,
               enable_com_classroom_plugin_incident_management,
               enable_com_mattermost_plugin_incident_management)                 as enable_incident_management
    , coalesce(enable_commattermostpluginincidentresponse,
               enable_com_mattermost_plugin_incident_response)                   as enable_incident_response
    , enable_jenkins
    , coalesce(enable_comgithubjespinorecommend, 
               enable_com_github_jespino_recommend)                              as enable_jespino_recommend
    , enable_jira
    , enable_jitsi
    , enable_marketplace
    , coalesce(enable_comgithubmatterpollmatterpoll,
               enable_com_github_matterpoll_matterpoll)                          as enable_materpoll
    , coalesce(enable_com_mattermost_agenda, 
               enable_commattermostagenda)                                       as enable_mattermost_agenda
    , enable_com_mattermost_apps                                                 as enable_mattermost_apps
    , enable_mattermost_plugin_azure_devops                                      as enable_mattermost_azure_devops
    , enable_com_mattermost_calls                                                as enable_mattermost_calls
    , enable_mattermost_plugin_hackerone                                         as enable_mattermost_hackerone
    , coalesce(enable_commattermostmsteamsmeetings,
               enable_com_mattermost_msteamsmeetings)                            as enable_mattermost_msteams_meetings
    , enable_com_mattermost_msteams_sync                                         as enable_mattermost_msteams_sync
    , coalesce(enable_mattermostprofanityfilter, 
               enable_mattermost_profanity_filter)                               as enable_mattermost_profanity_filter
    , enable_mattermost_plugin_servicenow                                        as enable_mattermost_servicenow
    , enable_mattermost_plugin_servicenow_virtual_agent                          as enable_mattermost_servicenow_virtual_agent
    , enable_memes
    , coalesce(enable_mscalendar, 
               enable_commattermostmscalendar,
               enable_com_classroom_mscalendar,
               enable_com_mattermost_mscalendar)                                 as enable_mscalendar
    , coalesce(enable_nps, 
               enable_com_mattermost_nps,
               enable_com_classroom_nps,
               enable_commattermostnps)                                          as enable_nps
    , enable_nps_survey
    , enable_playbooks
    , enable_remote_marketplace
    , enable_ru_loop_plugin_embeds
    , enable_ru_loop_plugin_scheduler
    , enable_ru_loop_plugin_user_fields
    , enable_set_default_theme
    , coalesce(enable_skype4business, 
               enable_skype_4_business)                                          as enable_skype4business
    , coalesce(enable_todo,
               enable_com_classroom_plugin_todo,
               enable_commattermostplugintodo,
               enable_com_mattermost_plugin_todo)                                as enable_todo
    , enable_uploads
    , coalesce(enable_com_mattermost_webex, 
               enable_commattermostwebex, 
               enable_webex,
               enable_com_classroom_webex)                                       as enable_webex 
    , coalesce(enable_welcome_bot,
               enable_com_classroom_welcomebot,
               enable_ru_loop_plugin_welcomebot,
               enable_commattermostwelcomebot,
               enable_com_mattermost_welcomebot)                                 as enable_welcome_bot
    , enable_zoom
    , is_default_marketplace_url
    , coalesce(require_pluginsignature,
               require_plugin_signature)                                         as require_plugin_signature
    , signature_public_key_files
    , version_alertmanager
    , version_antivirus
    , coalesce(version_autolink, 
               version_mattermost_autolink,
               version_mattermostautolink)                                       as version_autolink
    , coalesce(version_aws_sns, 
               version_com_mattermost_aws_sns,
               version_commattermostawssns)                                      as version_aws_sns
    , version_bitbucket
    , coalesce(version_commattermostpluginchannelexport,
               version_com_mattermost_plugin_channel_export)                     as version_channel_export
    , coalesce(version_comgithubmattermostplugincircleci,
               version_com_github_mattermost_plugin_circleci)                    as version_circleci
    , confluence(version_commattermostconfluence,
                 version_com_mattermost_confluence)                              as version_confluence
    , coalesce(version_commattermostcustomattributes,
               version_com_mattermost_custom_attributes,
               version_custom_user_attributes)                                   as version_custom_user_attributes
    , coalesce(version_comgithubmoussetcmattermostplugindiceroller,
               version_com_github_moussetc_mattermost_plugin_diceroller)         as version_diceroller
    , coalesce(version_comgithubphillipaherezamattermostplugindigitalocean,
               version_com_github_phillipahereza_mattermost_plugin_digitalocean) as version_digitalocean
    , version_focalboard
    , coalesce(version_comgithubmoussetcmattermostplugingiphy,
               version_com_github_moussetc_mattermost_plugin_giphy)              as version_giphy
    , version_github
    , coalesce(version_com_github_manland_mattermost_plugin_gitlab,
               version_comgithubmanlandmattermostplugingitlab,
               version_gitlab)                                                   as version_gitlab
    , coalesce(version_comnilsbrinkmannicebreaker,
               version_com_nilsbrinkmann_icebreaker)                             as version_icebreaker
    , coalesce(version_commattermostpluginincidentmanagement,
               version_com_mattermost_plugin_incident_management)                as version_incident_management
    , coalesce(version_commattermostpluginincidentresponse,
               version_com_mattermost_plugin_incident_response)                  as version_incident_response
    , version_jenkins
    , coalesce(version_comgithubjespinorecommend,
               version_com_github_jespino_recommend)                             as version_jespino_recommend
    , version_jira
    , version_jitsi
    , coalesce(version_comgithubmatterpollmatterpoll,
               version_com_github_matterpoll_matterpoll)                         as version_matterpoll
    , coalesce(version_com_mattermost_agenda,
               version_commattermostagenda)                                      as version_mattermost_agenda
    , version_com_mattermost_apps                                                as version_mattermost_apps
    , version_mattermost_plugin_azure_devops                                     as version_mattermost_azure_devops
    , version_com_mattermost_calls                                               as version_mattermost_calls
    , version_mattermost_plugin_hackerone                                        as version_mattermost_hackerone
    , coalesce(version_commattermostmsteamsmeetings,
               version_com_mattermost_msteamsmeetings)                           as version_mattermost_ms_teams_meetings
    , version_com_mattermost_msteams_sync                                        as version_mattermost_msteams_sync
    , coalesce(version_mattermostprofanityfilter, 
               version_mattermost_profanity_filter)                              as version_mattermost_profanity_filter
    , version_mattermost_plugin_servicenow                                       as version_mattermost_servicenow
    , version_mattermost_plugin_servicenow_virtual_agent                         as version_mattermost_servicenow_virtual_agent 
    , version_memes
    , coalesce(version_commattermostmscalendar,
               version_com_mattermost_mscalendar)                                as version_mscalendar
    , coalesce(version_com_mattermost_nps,
               version_commattermostnps, 
               version_nps)                                                      as version_nps
    , version_playbooks
    , version_set_default_theme
    , coalesce(version_skype4business, 
               version_skype_4_business)                                         as version_skype4business
    , coalesce(version_com_mattermost_plugin_todo,
               version_commattermostplugintodo)                                  as version_todo
    , coalesce(version_webex,
               version_commattermostwebex,
               version_com_mattermost_webex)                                     as version_webex
    , coalesce(version_commattermostwelcomebot,
               version_com_mattermost_welcomebot,
               version_welcome_bot)                                              as version_welcome_bot
    , version_zoom

    , context_library_name
    , context_library_version
    , sent_at
    , original_timestamp
from {{ ref('int_mm_telemetry_prod__config_saml') }}

