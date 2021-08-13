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
    FROM {{ source('mattermost2', 'config_timezone') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_timezone_details AS (
         SELECT
             timestamp::DATE                         AS date
           , t.user_id                               AS server_id
           , MAX(isdefault_supported_timezones_path) AS isdefault_supported_timezones_path
           , {{ dbt_utils.surrogate_key(['timestamp::date', 't.user_id']) }} AS id
         FROM {{ source('mattermost2', 'config_timezone') }} t
              JOIN max_timestamp          mt
                   ON t.user_id = mt.user_id
                       AND mt.max_timestamp = t.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_timezone_details