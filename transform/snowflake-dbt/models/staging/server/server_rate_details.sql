{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp        AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_rate') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_rate') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_rate_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , MAX(COALESCE(r.enable_rate_limiter)      AS enable_rate_limiter
           , MAX(COALESCE(r.isdefault_vary_by_header) AS isdefault_vary_by_header
           , MAX(COALESCE(r.max_burst)                AS max_burst
           , MAX(COALESCE(r.memory_store_size)        AS memory_store_size
           , MAX(COALESCE(r.per_sec)                  AS per_sec
           , MAX(COALESCE(r.vary_by_remote_address)   AS vary_by_remote_address
           , MAX(COALESCE(r.vary_by_user)             AS vary_by_user
           , {{ dbt_utils.surrogate_key('COALESCE(r.timestamp::DATE, s.timestamp::date)', 'COALESCE(r.user_id, s.user_id)') }} AS id
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_rate') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_rate') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2
     )
SELECT *
FROM server_rate_details