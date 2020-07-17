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
    FROM {{ source('mattermost2', 'config_rate') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_rate_details AS (
         SELECT
             timestamp::DATE               AS date
           , r.user_id                     AS server_id
           , MAX(enable_rate_limiter)      AS enable_rate_limiter
           , MAX(isdefault_vary_by_header) AS isdefault_vary_by_header
           , MAX(max_burst)                AS max_burst
           , MAX(memory_store_size)        AS memory_store_size
           , MAX(per_sec)                  AS per_sec
           , MAX(vary_by_remote_address)   AS vary_by_remote_address
           , MAX(vary_by_user)             AS vary_by_user
           , {{ dbt_utils.surrogate_key('timestamp::date', 'r.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_rate') }} r
              JOIN max_timestamp      mt
                   ON r.user_id = mt.user_id
                       AND mt.max_timestamp = r.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_rate_details