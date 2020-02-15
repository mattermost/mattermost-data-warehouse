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
    FROM {{ source('staging_config', 'config_notifications_log') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_notifications_log_details AS (
         SELECT
             timestamp::DATE                             AS date
           , n.user_id                                   AS server_id
           , max(enable_file)                            AS file_notification_logging
           , max(file_level)                             AS notification_log_file_level
           , max(file_json)                              AS notification_log_file_json
           , max(isdefault_file_location)                AS default_notification_log_file_location
           , max(enable_console)                         AS console_notification_logging
           , max(console_level)                          AS notification_log_console_level
           , max(console_json)                           AS notification_log_console_level
         FROM {{ source('staging_config', 'config_notifications_log') }} n
              JOIN max_timestamp                   mt
                   ON n.user_id = mt.user_id
                       AND mt.max_timestamp = n.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_notifications_log_details