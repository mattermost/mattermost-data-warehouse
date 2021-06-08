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
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

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
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_plugin_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, p.timestamp::DATE)                    AS date
           , COALESCE(s.user_id, p.user_id)                          AS server_id
           , MAX(COALESCE(s.allow_insecure_download_url, p.ALLOW_INSECURE_DOWNLOAD_URL))    AS allow_insecure_download_url
           , MAX(COALESCE(s.automatic_prepackaged_plugins, p.AUTOMATIC_PREPACKAGED_PLUGINS))  AS automatic_prepackaged_plugins
           , MAX(COALESCE(s.enable, p.ENABLE))                         AS enable_plugins
           , MAX(COALESCE(s.enable_antivirus, p.ENABLE_ANTIVIRUS))               AS enable_antivirus
           , MAX(COALESCE(s.enable_autolink, p.ENABLE_AUTOLINK, p.ENABLE_MATTERMOSTAUTOLINK))                AS enable_autolink
           , MAX(COALESCE(s.enable_aws_sns, p.ENABLE_AWS_SNS, p.ENABLE_COMMATTERMOSTAWSSNS))                 AS enable_aws_sns
           , MAX(COALESCE(s.enable_custom_user_attributes, p.ENABLE_CUSTOM_USER_ATTRIBUTES))  AS enable_custom_user_attributes
           , MAX(COALESCE(s.enable_github, p.ENABLE_GITHUB))                  AS enable_github
           , MAX(COALESCE(s.enable_gitlab, p.ENABLE_GITLAB, p.ENABLE_COMGITHUBMANLANDMATTERMOSTPLUGINGITLAB))         AS enable_gitlab
           , MAX(COALESCE(s.enable_health_check, p.ENABLE_HEALTH_CHECK))            AS enable_health_check
           , MAX(COALESCE(s.enable_jenkins, p.ENABLE_JENKINS))                 AS enable_jenkins
           , MAX(COALESCE(s.enable_jira, p.enable_jira))                    AS enable_jira
           , MAX(COALESCE(s.enable_marketplace, p.enable_marketplace))             AS enable_marketplace
           , MAX(COALESCE(s.enable_nps, p.enable_nps, p.ENABLE_COMMATTERMOSTNPS))                     AS enable_nps
           , MAX(COALESCE(s.enable_nps_survey, p.enable_nps_survey))              AS enable_nps_survey
           , MAX(COALESCE(s.enable_remote_marketplace, p.enable_remote_marketplace))      AS enable_remote_marketplace
           , MAX(COALESCE(s.enable_uploads, p.enable_uploads))                 AS enable_uploads
           , MAX(COALESCE(s.enable_webex, p.enable_webex, p.ENABLE_COMMATTERMOSTWEBEX))                   AS enable_webex
           , MAX(COALESCE(s.enable_welcome_bot, p.enable_welcome_bot, p.ENABLE_COMMATTERMOSTWELCOMEBOT))             AS enable_welcome_bot
           , MAX(COALESCE(s.enable_zoom, p.enable_zoom))                    AS enable_zoom
           , MAX(COALESCE(s.is_default_marketplace_url, p.is_default_marketplace_url))     AS is_default_marketplace_url
           , MAX(COALESCE(s.require_plugin_signature, p.require_pluginsignature))       AS require_plugin_signature
           , MAX(COALESCE(s.signature_public_key_files, p.signature_public_key_files))     AS signature_public_key_files
           , MAX(COALESCE(s.version_antivirus, p.version_antivirus))              AS version_antivirus
           , MAX(COALESCE(s.version_autolink, p.version_autolink, p.VERSION_MATTERMOSTAUTOLINK))               AS version_autolink
           , MAX(COALESCE(s.version_aws_sns, p.version_aws_sns, p.VERSION_COMMATTERMOSTAWSSNS))                AS version_aws_sns
           , MAX(COALESCE(s.version_custom_user_attributes, p.version_custom_user_attributes, p.VERSION_COMMATTERMOSTCUSTOMATTRIBUTES)) AS version_custom_user_attributes
           , MAX(COALESCE(s.version_github, p.version_github))                 AS version_github
           , MAX(COALESCE(s.version_gitlab, p.version_gitlab, p.VERSION_COMGITHUBMANLANDMATTERMOSTPLUGINGITLAB))                 AS version_gitlab
           , MAX(COALESCE(s.version_jenkins, p.version_jenkins))                AS version_jenkins
           , MAX(COALESCE(s.version_jira, p.version_jira))                   AS version_jira
           , MAX(COALESCE(s.version_nps, p.version_nps, p.VERSION_COMMATTERMOSTNPS))                    AS version_nps
           , MAX(COALESCE(s.version_webex, p.version_webex, p.VERSION_COMMATTERMOSTWEBEX))                  AS version_webex
           , MAX(COALESCE(s.version_welcome_bot, p.version_welcome_bot, p.VERSION_COMMATTERMOSTWELCOMEBOT))            AS version_welcome_bot
           , MAX(COALESCE(s.version_zoom, p.version_zoom))                   AS version_zoom
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, p.timestamp::date)', 'COALESCE(s.user_id, p.user_id)']) }} AS id
           , MAX(COALESCE(s.enable_confluence, p.enable_confluence, p.ENABLE_COMMATTERMOSTCONFLUENCE))              AS enable_confluence
           , MAX(COALESCE(s.enable_jitsi, p.enable_jitsi))                   AS enable_jitsi
           , MAX(COALESCE(s.enable_mscalendar, p.enable_mscalendar, p.ENABLE_COMMATTERMOSTMSCALENDAR))              AS enable_mscalendar
           , MAX(COALESCE(s.enable_todo, p.enable_todo, p.ENABLE_COMMATTERMOSTPLUGINTODO))                    AS enable_todo
           , MAX(COALESCE(s.enable_skype4business, p.enable_skype4business))          AS enable_skype4business
           , MAX(p.ENABLE_COMGITHUBMOUSSETCMATTERMOSTPLUGINGIPHY) AS enable_giphy
           , MAX(p.ENABLE_COMGITHUBPHILLIPAHEREZAMATTERMOSTPLUGINDIGITALOCEAN) AS enable_digital_ocean
           , MAX(p.ENABLE_COMMATTERMOSTPLUGININCIDENTRESPONSE)  AS enable_incident_response
           , MAX(p.enable_memes)  AS enable_memes
           , MAX(p.VERSION_COMGITHUBMOUSSETCMATTERMOSTPLUGINGIPHY) as version_giphy
           , MAX(p.VERSION_COMGITHUBPHILLIPAHEREZAMATTERMOSTPLUGINDIGITALOCEAN) as version_digital_ocean
           , MAX(p.VERSION_COMMATTERMOSTCONFLUENCE) as version_confluence
           , MAX(p.VERSION_COMMATTERMOSTMSCALENDAR) as version_mscalendar
           , MAX(p.VERSION_COMMATTERMOSTPLUGININCIDENTRESPONSE) as version_incident_response
           , MAX(p.VERSION_COMMATTERMOSTPLUGINTODO) as version_todo
           , MAX(p.VERSION_MEMES) as version_memes
           , MAX(p.VERSION_JITSI) as version_jitsi
           , MAX(p.VERSION_SKYPE4BUSINESS) as version_skype4business
           , MAX(COALESCE(p.CONTEXT_TRAITS_INSTALLATIONID, NULL))                   AS installation_id
           , MAX(COALESCE(p.enable_mattermostprofanityfilter, NULL))        AS enable_mattermostprofanityfilter
           , MAX(COALESCE(p.version_mattermostprofanityfilter, NULL))        AS version_mattermostprofanityfilter
           , MAX(COALESCE(p.enable_comgithubmatterpollmatterpoll, NULL))        AS enable_comgithubmatterpollmatterpoll
           , MAX(COALESCE(p.version_comgithubmatterpollmatterpoll, NULL))        AS version_comgithubmatterpollmatterpoll
           , MAX(COALESCE(p.enable_commattermostpluginincidentmanagement, NULL))        AS enable_commattermostpluginincidentmanagement
           , MAX(COALESCE(p.version_commattermostpluginincidentmanagement, NULL))        AS version_commattermostpluginincidentmanagement
           , MAX(COALESCE(p.enable_comgithubjespinorecommend, NULL))        AS enable_comgithubjespinorecommend
           , MAX(COALESCE(p.version_comgithubjespinorecommend, NULL))        AS version_comgithubjespinorecommend
           , MAX(COALESCE(p.enable_commattermostagenda, NULL))        AS enable_commattermostagenda
           , MAX(COALESCE(p.version_commattermostagenda, NULL))        AS version_commattermostagenda         
           , MAX(COALESCE(p.enable_commattermostmsteamsmeetings, NULL))        AS enable_commattermostmsteamsmeetings
           , MAX(COALESCE(p.enable_commattermostpluginchannelexport, NULL))        AS enable_commattermostpluginchannelexport
           , MAX(COALESCE(p.enable_comnilsbrinkmannicebreaker, NULL))        AS enable_comnilsbrinkmannicebreaker
           , MAX(COALESCE(p.version_commattermostmsteamsmeetings, NULL))        AS version_commattermostmsteamsmeetings
           , MAX(COALESCE(p.version_commattermostpluginchannelexport, NULL))        AS version_commattermostpluginchannelexport
           , MAX(COALESCE(p.version_comnilsbrinkmannicebreaker, NULL))        AS version_comnilsbrinkmannicebreaker
           , MAX(COALESCE(p.version_com_mattermost_apps, NULL))        AS version_mattermost_apps
           , MAX(COALESCE(p.enable_com_mattermost_apps, NULL))        AS enable_mattermost_apps
           , MAX(COALESCE(p.VERSION_COM_GITHUB_MATTERMOST_PLUGIN_CIRCLECI, VERSION_COMGITHUBMATTERMOSTPLUGINCIRCLECI))        AS version_circleci
           , MAX(COALESCE(p.ENABLE_COM_GITHUB_MATTERMOST_PLUGIN_CIRCLECI, p.ENABLE_COMGITHUBMATTERMOSTPLUGINCIRCLECI))        AS enable_circleci
           , MAX(COALESCE(p.VERSION_COMGITHUBMOUSSETCMATTERMOSTPLUGINDICEROLLER, p.VERSION_COM_GITHUB_MOUSSETC_MATTERMOST_PLUGIN_DICEROLLER))        AS version_diceroller
           , MAX(COALESCE(p.ENABLE_COM_GITHUB_MOUSSETC_MATTERMOST_PLUGIN_DICEROLLER, p.ENABLE_COMGITHUBMOUSSETCMATTERMOSTPLUGINDICEROLLER))        AS enable_diceroller
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
         GROUP BY 1, 2, 38
     )
SELECT *
FROM server_plugin_details