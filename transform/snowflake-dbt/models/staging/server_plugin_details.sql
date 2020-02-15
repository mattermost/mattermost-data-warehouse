{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , max(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_plugin') }}
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
           , max(enable)                        AS plugins_enabled
           , max(enable_antivirus)              AS antivirus_plugin
           , max(enable_autolink)               AS autolink_plugin
           , max(enable_aws_sns)                AS aws_sns_plugin
           , max(enable_custom_user_attributes) AS customer_user_attributes_plugin
           , max(enable_gitlab)                 AS gitlab_plugin
           , max(enable_github)                 AS github_plugin
           , max(enable_health_check)           AS health_check_plugin
           , max(enable_jenkins)                AS jenkins_plugin
           , max(enable_jira)                   AS jira_plugin
           , max(enable_marketplace)            AS marketplace_plugin
           , max(enable_nps)                    AS nps_plugin
           , max(enable_nps_survey)             AS nps_survey_plugin
           , max(enable_uploads)                AS uploads_plugin
           , max(enable_welcome_bot)            AS welcome_bot_plugin
           , max(enable_zoom)                   AS zoom_plugin
         FROM {{ source('staging_config', 'config_plugin') }} p
              JOIN max_timestamp        mt
                   ON p.user_id = mt.user_id
                       AND mt.max_timestamp = p.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_plugin_details