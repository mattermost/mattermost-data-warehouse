{{config({
    "materialized": "incremental",
    "schema": "mattermost",
    "unique_key":'id'
  })
}}


WITH max_flag_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ ref('feature_flag_telemetry') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),

server_feature_flag_details AS (
    SELECT ff.*
    FROM {{ ref('feature_flag_telemetry')}} ff
    JOIN max_flag_timestamp        mt
                   ON ff.user_id = mt.user_id
                       AND mt.max_timestamp = ff.timestamp
)

SELECT *
FROM server_feature_flag_details
