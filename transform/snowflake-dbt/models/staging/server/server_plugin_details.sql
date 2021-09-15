{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(s.timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_plugin') }} s
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
    FROM {{ source('mm_telemetry_prod', 'config_plugin') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_plugin_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, p.timestamp::DATE)                                                                                             AS date
           , COALESCE(s.user_id, p.user_id)                                                                                                             AS server_id
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, p.timestamp::date)', 'COALESCE(s.user_id, p.user_id)']) }} AS id
           , MAX(COALESCE(s.allow_insecure_download_url, p.ALLOW_INSECURE_DOWNLOAD_URL))                                                                AS allow_insecure_download_url
           , MAX(COALESCE(s.automatic_prepackaged_plugins, p.AUTOMATIC_PREPACKAGED_PLUGINS))                                                            AS automatic_prepackaged_plugins
           , MAX(COALESCE(s.is_default_marketplace_url, p.is_default_marketplace_url))                                                                  AS is_default_marketplace_url
           , MAX(COALESCE(s.require_plugin_signature, p.require_pluginsignature))                                                                       AS require_plugin_signature
           , MAX(COALESCE(s.signature_public_key_files, p.signature_public_key_files))                                                                  AS signature_public_key_files
           , MAX(COALESCE(p.CONTEXT_TRAITS_INSTALLATIONID, NULL))                                                                                       AS installation_id

           -- PLUGINS ENABLED FLAGS
           , MAX(COALESCE(s.enable, p.ENABLE))                                                                                                          AS enable_plugins
           , MAX(COALESCE(s.enable_antivirus, p.ENABLE_ANTIVIRUS))                                                                                      AS enable_antivirus
           , MAX(COALESCE(s.enable_autolink, p.ENABLE_AUTOLINK, p.ENABLE_MATTERMOSTAUTOLINK, p.ENABLE_MATTERMOST_AUTOLINK))                             AS enable_autolink
           , MAX(COALESCE(s.enable_aws_sns, p.ENABLE_AWS_SNS, p.ENABLE_COMMATTERMOSTAWSSNS, p.ENABLE_COM_MATTERMOST_AWS_SNS))                           AS enable_aws_sns
           , MAX(COALESCE(s.enable_custom_user_attributes, p.ENABLE_CUSTOM_USER_ATTRIBUTES, p.ENABLE_COM_MATTERMOST_CUSTOM_ATTRIBUTES))                 AS enable_custom_user_attributes
           , MAX(COALESCE(s.enable_github, p.ENABLE_GITHUB))                                                                                            AS enable_github
           , MAX(COALESCE(s.enable_gitlab, p.ENABLE_GITLAB, p.ENABLE_COMGITHUBMANLANDMATTERMOSTPLUGINGITLAB, 
                          p.ENABLE_COM_GITHUB_MANLAND_MATTERMOST_PLUGIN_GITLAB))                                                                        AS enable_gitlab
           , MAX(COALESCE(s.enable_health_check, p.ENABLE_HEALTH_CHECK))                                                                                AS enable_health_check
           , MAX(COALESCE(s.enable_jenkins, p.ENABLE_JENKINS))                                                                                          AS enable_jenkins
           , MAX(COALESCE(s.enable_jira, p.enable_jira))                                                                                                AS enable_jira
           , MAX(COALESCE(s.enable_marketplace, p.enable_marketplace))                                                                                  AS enable_marketplace
           , MAX(COALESCE(s.enable_nps, p.enable_nps, p.ENABLE_COMMATTERMOSTNPS, p.ENABLE_COM_MATTERMOST_NPS))                                          AS enable_nps
           , MAX(COALESCE(s.enable_nps_survey, p.enable_nps_survey))                                                                                    AS enable_nps_survey
           , MAX(COALESCE(s.enable_remote_marketplace, p.enable_remote_marketplace))                                                                    AS enable_remote_marketplace
           , MAX(COALESCE(s.enable_uploads, p.enable_uploads))                                                                                          AS enable_uploads
           , MAX(COALESCE(s.enable_webex, p.enable_webex, p.ENABLE_COMMATTERMOSTWEBEX, p.ENABLE_COM_MATTERMOST_WEBEX))                                  AS enable_webex
           , MAX(COALESCE(s.enable_welcome_bot, p.enable_welcome_bot, p.ENABLE_COMMATTERMOSTWELCOMEBOT, p.ENABLE_COM_MATTERMOST_WELCOMEBOT))            AS enable_welcome_bot
           , MAX(COALESCE(s.enable_zoom, p.enable_zoom))                                                                                                AS enable_zoom
           , MAX(COALESCE(s.enable_confluence, p.enable_confluence, p.ENABLE_COMMATTERMOSTCONFLUENCE, p.ENABLE_COM_MATTERMOST_CONFLUENCE))              AS enable_confluence
           , MAX(COALESCE(s.enable_jitsi, p.enable_jitsi))                                                                                              AS enable_jitsi
           , MAX(COALESCE(s.enable_mscalendar, p.enable_mscalendar, p.ENABLE_COMMATTERMOSTMSCALENDAR, p.ENABLE_COM_MATTERMOST_MSCALENDAR))              AS enable_mscalendar
           , MAX(COALESCE(s.enable_todo, p.enable_todo, p.ENABLE_COMMATTERMOSTPLUGINTODO, p.ENABLE_COM_MATTERMOST_PLUGIN_TODO))                         AS enable_todo
           , MAX(COALESCE(s.enable_skype4business, p.enable_skype4business, p.ENABLE_SKYPE_4_BUSINESS))                                                 AS enable_skype4business
           , MAX(COALESCE(p.ENABLE_COMGITHUBMOUSSETCMATTERMOSTPLUGINGIPHY, p.ENABLE_COM_GITHUB_MOUSSETC_MATTERMOST_PLUGIN_GIPHY))                       AS enable_giphy
           , MAX(COALESCE(p.ENABLE_COMGITHUBPHILLIPAHEREZAMATTERMOSTPLUGINDIGITALOCEAN, p.ENABLE_COM_GITHUB_PHILLIPAHEREZA_MATTERMOST_PLUGIN_DIGITALOCEAN, 
                  p.VERSION_COM_GITHUB_PHILLIPAHEREZA_MATTERMOST_PLUGIN_DIGITALOCEAN))                                                                  AS enable_digital_ocean
           , MAX(COALESCE(p.ENABLE_COMMATTERMOSTPLUGININCIDENTRESPONSE, p.ENABLE_COM_MATTERMOST_PLUGIN_INCIDENT_RESPONSE))                              AS enable_incident_response
           , MAX(p.enable_memes)                                                                                                                        AS enable_memes
           , MAX(COALESCE(p.enable_mattermostprofanityfilter, p.ENABLE_MATTERMOST_PROFANITY_FILTER))                                                    AS enable_mattermostprofanityfilter
           , MAX(COALESCE(p.enable_comgithubmatterpollmatterpoll, p.ENABLE_COM_GITHUB_MATTERPOLL_MATTERPOLL))                                           AS enable_comgithubmatterpollmatterpoll
           , MAX(COALESCE(p.ENABLE_COM_GITHUB_MOUSSETC_MATTERMOST_PLUGIN_DICEROLLER, p.ENABLE_COMGITHUBMOUSSETCMATTERMOSTPLUGINDICEROLLER))             AS enable_diceroller
           , MAX(COALESCE(p.enable_commattermostpluginincidentmanagement, p.ENABLE_COM_MATTERMOST_PLUGIN_INCIDENT_MANAGEMENT))                          AS enable_commattermostpluginincidentmanagement
           , MAX(COALESCE(p.enable_comgithubjespinorecommend, p.ENABLE_COM_GITHUB_JESPINO_RECOMMEND))                                                   AS enable_comgithubjespinorecommend
           , MAX(COALESCE(p.enable_commattermostagenda, p.ENABLE_COM_MATTERMOST_AGENDA))                                                                AS enable_commattermostagenda
           , MAX(COALESCE(p.enable_commattermostmsteamsmeetings, p.ENABLE_COM_MATTERMOST_MSTEAMSMEETINGS))                                              AS enable_commattermostmsteamsmeetings
           , MAX(COALESCE(p.enable_commattermostpluginchannelexport, p.ENABLE_COM_MATTERMOST_PLUGIN_CHANNEL_EXPORT))                                    AS enable_commattermostpluginchannelexport 
           , MAX(COALESCE(p.enable_comnilsbrinkmannicebreaker, p.ENABLE_COM_NILSBRINKMANN_ICEBREAKER))                                                  AS enable_comnilsbrinkmannicebreaker
           , MAX(COALESCE(p.enable_com_mattermost_apps, NULL))                                                                                          AS enable_mattermost_apps
           , MAX(COALESCE(p.ENABLE_COM_GITHUB_MATTERMOST_PLUGIN_CIRCLECI, p.ENABLE_COMGITHUBMATTERMOSTPLUGINCIRCLECI))                                  AS enable_circleci
           , MAX(COALESCE(p.enable_focalboard, null))                                                                                                   AS enable_focalboard

           -- VERSIONS 
           , MAX(COALESCE(s.version_antivirus, p.version_antivirus))                                                                                    AS version_antivirus
           , MAX(COALESCE(s.version_autolink, p.version_autolink, p.VERSION_MATTERMOSTAUTOLINK, p.VERSION_MATTERMOST_AUTOLINK))                         AS version_autolink
           , MAX(COALESCE(s.version_aws_sns, p.version_aws_sns, p.VERSION_COMMATTERMOSTAWSSNS, p.VERSION_COM_MATTERMOST_AWS_SNS))                       AS version_aws_sns
           , MAX(COALESCE(s.version_custom_user_attributes, p.version_custom_user_attributes, p.VERSION_COMMATTERMOSTCUSTOMATTRIBUTES, 
                          p.VERSION_COM_MATTERMOST_CUSTOM_ATTRIBUTES))                                                                                  AS version_custom_user_attributes
           , MAX(COALESCE(s.version_github, p.version_github))                                                                                          AS version_github
           , MAX(COALESCE(s.version_gitlab, p.version_gitlab, p.VERSION_COMGITHUBMANLANDMATTERMOSTPLUGINGITLAB, 
                            p.VERSION_COM_GITHUB_MANLAND_MATTERMOST_PLUGIN_GITLAB))                                                                     AS version_gitlab
           , MAX(COALESCE(s.version_jenkins, p.version_jenkins))                                                                                        AS version_jenkins
           , MAX(COALESCE(s.version_jira, p.version_jira))                                                                                              AS version_jira
           , MAX(COALESCE(s.version_nps, p.version_nps, p.VERSION_COMMATTERMOSTNPS, p.VERSION_COM_MATTERMOST_NPS))                                      AS version_nps
           , MAX(COALESCE(s.version_webex, p.version_webex, p.VERSION_COMMATTERMOSTWEBEX, p.VERSION_COM_MATTERMOST_WEBEX))                              AS version_webex
           , MAX(COALESCE(s.version_welcome_bot, p.version_welcome_bot, p.VERSION_COMMATTERMOSTWELCOMEBOT, p.VERSION_COM_MATTERMOST_WELCOMEBOT))        AS version_welcome_bot
           , MAX(COALESCE(s.version_zoom, p.version_zoom))                                                                                              AS version_zoom
           , MAX(COALESCE(p.VERSION_COMGITHUBMOUSSETCMATTERMOSTPLUGINGIPHY, p.VERSION_COM_GITHUB_MOUSSETC_MATTERMOST_PLUGIN_GIPHY))                     AS version_giphy
           , MAX(COALESCE(p.VERSION_COMGITHUBPHILLIPAHEREZAMATTERMOSTPLUGINDIGITALOCEAN, 
                          p.VERSION_COM_GITHUB_PHILLIPAHEREZA_MATTERMOST_PLUGIN_DIGITALOCEAN))                                                          AS version_digital_ocean
           , MAX(COALESCE(p.VERSION_COMMATTERMOSTCONFLUENCE, p.VERSION_COM_MATTERMOST_CONFLUENCE))                                                      AS version_confluence
           , MAX(COALESCE(p.VERSION_COMMATTERMOSTMSCALENDAR, p.VERSION_COM_MATTERMOST_MSCALENDAR))                                                      AS version_mscalendar
           , MAX(COALESCE(p.VERSION_COMMATTERMOSTPLUGININCIDENTRESPONSE, p.VERSION_COM_MATTERMOST_PLUGIN_INCIDENT_RESPONSE))                            AS version_incident_response
           , MAX(COALESCE(p.VERSION_COMMATTERMOSTPLUGINTODO, p.VERSION_COM_MATTERMOST_PLUGIN_TODO))                                                     AS version_todo
           , MAX(COALESCE(p.VERSION_MEMES, NULL))                                                                                                       AS version_memes
           , MAX(COALESCE(p.VERSION_JITSI, NULL))                                                                                                       AS version_jitsi
           , MAX(COALESCE(p.VERSION_SKYPE4BUSINESS,  p.VERSION_SKYPE_4_BUSINESS))                                                                       AS version_skype4business
           , MAX(COALESCE(p.version_mattermostprofanityfilter, p.VERSION_MATTERMOST_PROFANITY_FILTER))                                                  AS version_mattermostprofanityfilter
           , MAX(COALESCE(p.version_comgithubmatterpollmatterpoll, p.VERSION_COM_GITHUB_MATTERPOLL_MATTERPOLL))                                         AS version_comgithubmatterpollmatterpoll
           , MAX(COALESCE(p.version_commattermostpluginincidentmanagement, p.VERSION_COM_MATTERMOST_PLUGIN_INCIDENT_MANAGEMENT))                        AS version_commattermostpluginincidentmanagement
           , MAX(COALESCE(p.version_comgithubjespinorecommend, p.VERSION_COM_GITHUB_JESPINO_RECOMMEND))                                                 AS version_comgithubjespinorecommend
           , MAX(COALESCE(p.version_commattermostagenda, p.VERSION_COM_MATTERMOST_AGENDA))                                                              AS version_commattermostagenda        
           , MAX(COALESCE(p.version_commattermostmsteamsmeetings, p.VERSION_COM_MATTERMOST_MSTEAMSMEETINGS))                                            AS version_commattermostmsteamsmeetings
           , MAX(COALESCE(p.version_commattermostpluginchannelexport, p.VERSION_COM_MATTERMOST_PLUGIN_CHANNEL_EXPORT))                                  AS version_commattermostpluginchannelexport
           , MAX(COALESCE(p.version_comnilsbrinkmannicebreaker, VERSION_COM_NILSBRINKMANN_ICEBREAKER))                                                  AS version_comnilsbrinkmannicebreaker
           , MAX(COALESCE(p.version_com_mattermost_apps, NULL))                                                                                         AS version_mattermost_apps
           , MAX(COALESCE(p.VERSION_COM_GITHUB_MATTERMOST_PLUGIN_CIRCLECI, p.VERSION_COMGITHUBMATTERMOSTPLUGINCIRCLECI))                                AS version_circleci
           , MAX(COALESCE(p.VERSION_COMGITHUBMOUSSETCMATTERMOSTPLUGINDICEROLLER, p.VERSION_COM_GITHUB_MOUSSETC_MATTERMOST_PLUGIN_DICEROLLER))           AS version_diceroller
           , MAX(COALESCE(p.version_focalboard, NULL))           AS version_focalboard
           -- Chimera OAUTH Proxy URL
           , MAX(COALESCE(p.chimera_oauth_proxy_url, NULL))           AS chimera_oauth_proxy_url
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_plugin') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_plugin') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) p
            ON s.timestamp::date = p.timestamp::date
            AND s.user_id = p.user_id
         GROUP BY 1, 2, 3
     )
SELECT *
FROM server_plugin_details