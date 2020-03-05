{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_plugin') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_plugin_details AS (
         SELECT
             timestamp::DATE                    AS date
           , p.user_id                          AS server_id
           , MAX(allow_insecure_download_url)    AS allow_insecure_download_url
           , MAX(automatic_prepackaged_plugins)  AS automatic_prepackaged_plugins
           , MAX(enable)                         AS enable_plugins
           , MAX(enable_antivirus)               AS enable_antivirus
           , MAX(enable_autolink)                AS enable_autolink
           , MAX(enable_aws_sns)                 AS enable_aws_sns
           , MAX(enable_custom_user_attributes)  AS enable_custom_user_attributes
           , MAX(enable_github)                  AS enable_github
           , MAX(enable_gitlab)                  AS enable_gitlab
           , MAX(enable_health_check)            AS enable_health_check
           , MAX(enable_jenkins)                 AS enable_jenkins
           , MAX(enable_jira)                    AS enable_jira
           , MAX(enable_marketplace)             AS enable_marketplace
           , MAX(enable_nps)                     AS enable_nps
           , MAX(enable_nps_survey)              AS enable_nps_survey
           , MAX(enable_remote_marketplace)      AS enable_remote_marketplace
           , MAX(enable_uploads)                 AS enable_uploads
           , MAX(enable_webex)                   AS enable_webex
           , MAX(enable_welcome_bot)             AS enable_welcome_bot
           , MAX(enable_zoom)                    AS enable_zoom
           , MAX(is_default_marketplace_url)     AS is_default_marketplace_url
           , MAX(require_plugin_signature)       AS require_plugin_signature
           , MAX(signature_public_key_files)     AS signature_public_key_files
           , MAX(version_antivirus)              AS version_antivirus
           , MAX(version_autolink)               AS version_autolink
           , MAX(version_aws_sns)                AS version_aws_sns
           , MAX(version_custom_user_attributes) AS version_custom_user_attributes
           , MAX(version_github)                 AS version_github
           , MAX(version_gitlab)                 AS version_gitlab
           , MAX(version_jenkins)                AS version_jenkins
           , MAX(version_jira)                   AS version_jira
           , MAX(version_nps)                    AS version_nps
           , MAX(version_webex)                  AS version_webex
           , MAX(version_welcome_bot)            AS version_welcome_bot
           , MAX(version_zoom)                   AS version_zoom
         FROM {{ source('mattermost2', 'config_plugin') }} p
              JOIN max_timestamp        mt
                   ON p.user_id = mt.user_id
                       AND mt.max_timestamp = p.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_plugin_details