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
    FROM {{ source('staging_config', 'config_timezone') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_timezone_details AS (
         SELECT
             timestamp::DATE                         AS date
           , t.user_id                               AS server_id
           , MAX(isdefault_supported_timezones_path) AS isdefault_supported_timezones_path
         FROM {{ source('staging_config', 'config_timezone') }} t
              JOIN max_timestamp          mt
                   ON t.user_id = mt.user_id
                       AND mt.max_timestamp = t.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_timezone_details