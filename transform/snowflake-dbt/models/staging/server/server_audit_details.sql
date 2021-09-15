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
    FROM {{ source('mattermost2', 'config_audit') }}
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
    FROM {{ source('mm_telemetry_prod', 'config_audit') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_audit_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
            , MAX(COALESCE(r.FILE_COMPRESS, s.FILE_COMPRESS)) AS FILE_COMPRESS
            , MAX(COALESCE(r.FILE_ENABLED, s.FILE_ENABLED)) AS FILE_ENABLED
            , MAX(COALESCE(r.FILE_MAX_AGE_DAYS, s.FILE_MAX_AGE_DAYS)) AS FILE_MAX_AGE_DAYS
            , MAX(COALESCE(r.FILE_MAX_BACKUPS, s.FILE_MAX_BACKUPS)) AS FILE_MAX_BACKUPS
            , MAX(COALESCE(r.FILE_MAX_QUEUE_SIZE, s.FILE_MAX_QUEUE_SIZE)) AS FILE_MAX_QUEUE_SIZE
            , MAX(COALESCE(r.FILE_MAX_SIZE_MB, s.FILE_MAX_SIZE_MB)) AS FILE_MAX_SIZE_MB
            , MAX(COALESCE(r.SYSLOG_ENABLED, s.SYSLOG_ENABLED)) AS SYSLOG_ENABLED
            , MAX(COALESCE(r.SYSLOG_INSECURE, s.SYSLOG_INSECURE)) AS SYSLOG_INSECURE
            , MAX(COALESCE(r.SYSLOG_MAX_QUEUE_SIZE, s.SYSLOG_MAX_QUEUE_SIZE)) AS SYSLOG_MAX_QUEUE_SIZE
           , {{ dbt_utils.surrogate_key(['COALESCE(r.timestamp::DATE, s.timestamp::date)', 'COALESCE(r.user_id, s.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           , MAX(COALESCE(r.ADVANCED_LOGGING_CONFIG, NULL))     AS ADVANCED_LOGGING_CONFIG
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_audit') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_audit') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 12, 13
     )
SELECT *
FROM server_audit_details