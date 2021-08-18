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
    FROM {{ source('mattermost2', 'config_webrtc') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_webrtc_details AS (
         SELECT
             timestamp::DATE         AS date
           , w.user_id               AS server_id
           , MAX(enable)             AS enable
           , MAX(isdefault_stun_uri) AS isdefault_stun_uri
           , MAX(isdefault_turn_uri) AS isdefault_turn_uri
           , {{ dbt_utils.surrogate_key(['timestamp::date', 'w.user_id']) }} AS id
         FROM {{ source('mattermost2', 'config_webrtc') }} w
              JOIN max_timestamp        mt
                   ON w.user_id = mt.user_id
                       AND mt.max_timestamp = w.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_webrtc_details