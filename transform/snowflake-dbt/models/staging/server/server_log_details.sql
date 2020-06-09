{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_log') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_log_details AS (
         SELECT
             timestamp::DATE               AS date
           , l.user_id                     AS server_id
           , MAX(console_json)             AS console_json
           , MAX(console_level)            AS console_level
           , MAX(enable_console)           AS enable_console
           , MAX(enable_file)              AS enable_file
           , MAX(enable_webhook_debugging) AS enable_webhook_debugging
           , MAX(file_json)                AS file_json
           , MAX(file_level)               AS file_level
           , MAX(isdefault_file_format)    AS isdefault_file_format
           , MAX(isdefault_file_location)  AS isdefault_file_location
           , {{ dbt_utils.surrogate_key('timestamp::date', 'l.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_log') }} l
              JOIN max_timestamp     mt
                   ON l.user_id = mt.user_id
                       AND mt.max_timestamp = l.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_log_details