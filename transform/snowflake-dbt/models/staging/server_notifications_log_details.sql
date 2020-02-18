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
    FROM {{ source('staging_config', 'config_notifications_log') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_notifications_log_details AS (
         SELECT
             timestamp::DATE              AS date
           , n.user_id                    AS server_id
           , MAX(console_json)            AS console_json
           , MAX(console_level)           AS console_level
           , MAX(enable_console)          AS enable_console
           , MAX(enable_file)             AS enable_file
           , MAX(file_json)               AS file_json
           , MAX(file_level)              AS file_level
           , MAX(isdefault_file_location) AS isdefault_file_location
         FROM {{ source('staging_config', 'config_notifications_log') }} n
              JOIN max_timestamp                   mt
                   ON n.user_id = mt.user_id
                       AND mt.max_timestamp = n.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_notifications_log_details