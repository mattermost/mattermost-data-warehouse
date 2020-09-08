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
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

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
           , {{ dbt_utils.surrogate_key('COALESCE(r.timestamp::DATE, NULL)', 'COALESCE(r.user_id, NULL)') }} AS id
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