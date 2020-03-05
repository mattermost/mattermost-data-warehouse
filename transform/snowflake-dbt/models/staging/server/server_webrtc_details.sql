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
    FROM {{ source('mattermost2', 'config_webrtc') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

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
         FROM {{ source('mattermost2', 'config_webrtc') }} w
              JOIN max_timestamp        mt
                   ON w.user_id = mt.user_id
                       AND mt.max_timestamp = w.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_webrtc_details