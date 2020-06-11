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
    FROM {{ source('mattermost2', 'config_metrics') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_metrics_details AS (
         SELECT
             timestamp::DATE         AS date
           , m.user_id               AS server_id
           , MAX(block_profile_rate) AS block_profile_rate
           , MAX(enable)             AS enable_metrics
           , {{ dbt_utils.surrogate_key('timestamp::date', 'm.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_metrics') }} m
              JOIN max_timestamp         mt
                   ON m.user_id = mt.user_id
                       AND mt.max_timestamp = m.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_metrics_details