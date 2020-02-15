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
    FROM {{ source('staging_config', 'config_rate') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_rate_details AS (
         SELECT
             timestamp::DATE               AS date
           , r.user_id                     AS server_id
           , max(max_burst)                AS max_burst
           , max(isdefault_vary_by_header) AS isdefault_vary_by_header
           , max(memory_store_size)        AS memory_store_size
           , max(per_sec)                  AS per_sec
           , max(vary_by_remote_address)   AS vary_by_remote_address
           , max(vary_by_user)             AS vary_by_user
           , max(enable_rate_limiter)      AS enable_rate_limiter
         FROM {{ source('staging_config', 'config_rate') }} r
              JOIN max_timestamp      mt
                   ON r.user_id = mt.user_id
                       AND mt.max_timestamp = r.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_rate_details