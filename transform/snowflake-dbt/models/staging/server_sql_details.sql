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
    FROM {{ source('staging_config', 'config_sql') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_sql_details AS (
         SELECT
             timestamp::DATE                             AS date
           , s.user_id                                   AS server_id
           , MAX(conn_max_lifetime_milliseconds)         AS conn_max_lifetime_milliseconds
           , MAX(data_source_replicas)                   AS data_source_replicas
           , MAX(data_source_search_replicas)            AS data_source_search_replicas
           , MAX(driver_name)                            AS driver_name
           , MAX(enable_public_channels_materialization) AS enable_public_channels_materialization
           , MAX(max_idle_conns)                         AS max_idle_conns
           , MAX(max_open_conns)                         AS max_open_conns
           , MAX(query_timeout)                          AS query_timeout
           , MAX(trace)                                  AS trace
         FROM {{ source('staging_config', 'config_sql') }} s
              JOIN max_timestamp     mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_sql_details