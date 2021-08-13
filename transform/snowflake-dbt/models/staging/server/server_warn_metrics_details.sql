{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_rudder_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'warn_metrics') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_warn_metrics_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, NULL)                        AS date
           , COALESCE(r.user_id, NULL)                                AS server_id
           , MAX(COALESCE(r.WARN_METRIC_NUMBER_OF_ACTIVE_USERS_200, NULL)) AS WARN_METRIC_NUMBER_OF_ACTIVE_USERS_200
           , MAX(COALESCE(r.WARN_METRIC_NUMBER_OF_ACTIVE_USERS_400, NULL)) AS WARN_METRIC_NUMBER_OF_ACTIVE_USERS_400
           , MAX(COALESCE(r.WARN_METRIC_NUMBER_OF_ACTIVE_USERS_500, NULL)) AS WARN_METRIC_NUMBER_OF_ACTIVE_USERS_500
           , {{ dbt_utils.surrogate_key(['COALESCE(r.timestamp::DATE, NULL)', 'COALESCE(r.user_id, NULL)']) }} AS id
           , MAX(COALESCE(r.warn_metric_email_domain, NULL))        AS warn_metric_email_domain
           , MAX(COALESCE(r.warn_metric_mfa, NULL))        AS warn_metric_mfa
           , MAX(COALESCE(r.warn_metric_number_of_teams_5, NULL))        AS warn_metric_number_of_teams_5
           , MAX(COALESCE(r.warn_metric_number_of_active_users_100, NULL))        AS warn_metric_number_of_active_users_100
           , MAX(COALESCE(r.warn_metric_number_of_active_users_300, NULL))        AS warn_metric_number_of_active_users_300
           , MAX(COALESCE(r.warn_metric_number_of_channels_50, NULL))        AS warn_metric_number_of_channels_50
           , MAX(COALESCE(r.warn_metric_number_of_posts_2m, NULL))        AS warn_metric_number_of_posts_2m
         FROM 
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'warn_metrics') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
         GROUP BY 1, 2
     )
SELECT *
FROM server_warn_metrics_details