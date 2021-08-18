{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp                 AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_data_retention') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp                 AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_data_retention') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

     server_data_retention_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::date)              AS date
           , COALESCE(s.user_id, r.user_id)                              AS server_id
           , MAX(COALESCE(s.message_retention_days, r.message_retention_days))  AS message_retention_days
           , MAX(COALESCE(s.file_retention_days, r.file_retention_days))     AS file_retention_days
           , MAX(COALESCE(s.enable_message_deletion, r.enable_message_deletion)) AS enable_message_deletion
           , MAX(COALESCE(s.enable_file_deletion, r.enable_file_deletion))    AS enable_file_deletion
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_data_retention') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_data_retention') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 7, 8
     )
SELECT *
FROM server_data_retention_details