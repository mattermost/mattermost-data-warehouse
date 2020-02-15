{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp                 AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , max(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_data_retention') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_data_retention_details AS (
         SELECT
             timestamp::DATE              AS date
           , dr.user_id                   AS server_id
           , max(message_retention_days)  AS message_retention_day
           , max(file_retention_days)     AS file_retention_days
           , max(enable_message_deletion) AS enable_message_deletion
           , max(enable_file_deletion)    AS enable_file_deletion
         FROM {{ source('staging_config', 'config_data_retention') }} dr
              JOIN max_timestamp                mt
                   ON dr.user_id = mt.user_id
                       AND mt.max_timestamp = dr.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_data_retention_details