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
    FROM {{ source('staging_config', 'config_log') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_log_details AS (
         SELECT
             timestamp::DATE                             AS date
           , l.user_id                                   AS server_id
           , max(enable_console)                         AS console_logging
           , max(console_level)                          AS log_console_level
           , max(console_json)                           AS log_console_json
           , max(enable_file)                            AS file_logging
           , max(file_level)                             AS log_file_level
           , max(file_json)                              AS log_file_json
           , max(isdefault_file_location)                AS isdefault_file_log_location
           , max(isdefault_file_format)                  AS isdefault_file_log_format
           , max(enable_webhook_debugging)               AS enable_webhook_debugging
         FROM {{ source('staging_config', 'config_log') }} l
              JOIN max_timestamp     mt
                   ON l.user_id = mt.user_id
                       AND mt.max_timestamp = l.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_log_details