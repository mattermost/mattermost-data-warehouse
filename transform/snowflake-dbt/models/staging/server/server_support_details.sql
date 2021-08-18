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
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_support') }}
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
    FROM {{ source('mm_telemetry_prod', 'config_support') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

     server_support_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::DATE)                 AS date
           , COALESCE(s.user_id, r.user_id)                                 AS server_id
           , MAX(s.custom_service_terms_enabled)                            AS custom_service_terms_enabled
           , MAX(COALESCE(s.custom_terms_of_service_enabled, 
                          r.custom_terms_of_service_enabled, 
                          s.custom_service_terms_enabled))                  AS custom_terms_of_service_enabled
           , MAX(COALESCE(s.custom_terms_of_service_re_acceptance_period, 
                          r.custom_terms_of_service_re_acceptance_period))  AS custom_terms_of_service_re_acceptance_period
           , MAX(COALESCE(s.isdefault_about_link, 
                          r.isdefault_about_link))                          AS isdefault_about_link
           , MAX(COALESCE(s.isdefault_help_link, r.isdefault_help_link))    AS isdefault_help_link
           , MAX(COALESCE(s.isdefault_privacy_policy_link, 
                          r.isdefault_privacy_policy_link))                 AS isdefault_privacy_policy_link
           , MAX(COALESCE(s.isdefault_report_a_problem_link, 
                          r.isdefault_report_a_problem_link))               AS isdefault_report_a_problem_link
           , MAX(COALESCE(s.isdefault_support_email, 
                          r.isdefault_support_email))                       AS isdefault_support_email
           , MAX(COALESCE(s.isdefault_terms_of_service_link, 
                          r.isdefault_terms_of_service_link))               AS isdefault_terms_of_service_link
           , MAX(s.segment_dedupe_id)                              AS segment_dedupe_id
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , MAX(r.enable_ask_community_link)                               AS enable_ask_community_link           
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
         FROM (
                SELECT s.*
                FROM {{ source('mattermost2', 'config_support') }} s
                JOIN max_segment_timestamp         mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
              ) s
         FULL OUTER JOIN 
              (
                SELECT r.*
                FROM {{ source('mm_telemetry_prod', 'config_support') }} r
                JOIN max_rudder_timestamp         mt
                   ON r.user_id = mt.user_id
                       AND mt.max_timestamp = r.timestamp
              ) r
         ON s.user_id = r.user_id
         AND s.timestamp::date = r.timestamp::date
         GROUP BY 1, 2, 13, 15
     )
SELECT *
FROM server_support_details