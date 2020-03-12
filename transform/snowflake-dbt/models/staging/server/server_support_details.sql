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
    FROM {{ source('mattermost2', 'config_support') }}
    WHERE timestamp::DATE <= CURRENT_DATE - INTERVAL '1 DAY'
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_support_details AS (
         SELECT
             timestamp::DATE                                   AS date
           , s.user_id                                         AS server_id
           , MAX(custom_service_terms_enabled)                 AS custom_service_terms_enabled
           , MAX(custom_terms_of_service_enabled)              AS custom_terms_of_service_enabled
           , MAX(custom_terms_of_service_re_acceptance_period) AS custom_terms_of_service_re_acceptance_period
           , MAX(isdefault_about_link)                         AS isdefault_about_link
           , MAX(isdefault_help_link)                          AS isdefault_help_link
           , MAX(isdefault_privacy_policy_link)                AS isdefault_privacy_policy_link
           , MAX(isdefault_report_a_problem_link)              AS isdefault_report_a_problem_link
           , MAX(isdefault_support_email)                      AS isdefault_support_email
           , MAX(isdefault_terms_of_service_link)              AS isdefault_terms_of_service_link
           , MAX(segment_dedupe_id)                            AS segment_dedupe_id
         FROM {{ source('mattermost2', 'config_support') }} s
              JOIN max_timestamp         mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_support_details