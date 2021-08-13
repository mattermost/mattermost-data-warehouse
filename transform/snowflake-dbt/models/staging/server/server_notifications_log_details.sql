{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp        AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_notifications_log') }}
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
    FROM {{ source('mm_telemetry_prod', 'config_notifications_log') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_notifications_log_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , MAX(COALESCE(r.console_json, s.console_json))            AS console_json
           , MAX(COALESCE(r.console_level, s.console_level))           AS console_level
           , MAX(COALESCE(r.enable_console, s.enable_console))          AS enable_console
           , MAX(COALESCE(r.enable_file, s.enable_file))             AS enable_file
           , MAX(COALESCE(r.file_json, s.file_json))               AS file_json
           , MAX(COALESCE(r.file_level, s.file_level))              AS file_level
           , MAX(COALESCE(r.isdefault_file_location, s.isdefault_file_location)) AS isdefault_file_location           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           , MAX(COALESCE(r.advanced_logging_config, NULL)) AS advanced_logging_config   
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_notifications_log') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_notifications_log') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 10, 11
     )
SELECT *
FROM server_notifications_log_details