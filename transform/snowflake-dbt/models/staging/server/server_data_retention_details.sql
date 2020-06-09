{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_timestamp                 AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_data_retention') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_data_retention_details AS (
         SELECT
             timestamp::DATE              AS date
           , dr.user_id                   AS server_id
           , MAX(message_retention_days)  AS message_retention_days
           , MAX(file_retention_days)     AS file_retention_days
           , MAX(enable_message_deletion) AS enable_message_deletion
           , MAX(enable_file_deletion)    AS enable_file_deletion
           , {{ dbt_utils.surrogate_key('timestamp::date', 'dr.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_data_retention') }} dr
              JOIN max_timestamp                mt
                   ON dr.user_id = mt.user_id
                       AND mt.max_timestamp = dr.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_data_retention_details