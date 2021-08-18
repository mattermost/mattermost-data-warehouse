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
    FROM {{ source('mattermost2', 'config_sql') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_sql') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_sql_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , MAX(COALESCE(r.conn_max_lifetime_milliseconds, s.conn_max_lifetime_milliseconds))         AS conn_max_lifetime_milliseconds
           , MAX(COALESCE(r.data_source_replicas, s.data_source_replicas))                   AS data_source_replicas
           , MAX(COALESCE(r.data_source_search_replicas, s.data_source_search_replicas))            AS data_source_search_replicas
           , MAX(COALESCE(r.driver_name, s.driver_name))                            AS driver_name
           , MAX(COALESCE(s.enable_public_channels_materialization, NULL )) AS enable_public_channels_materialization
           , MAX(COALESCE(r.max_idle_conns, s.max_idle_conns))                         AS max_idle_conns
           , MAX(COALESCE(r.max_open_conns, s.max_open_conns))                         AS max_open_conns
           , MAX(COALESCE(r.query_timeout, s.query_timeout))                          AS query_timeout
           , MAX(COALESCE(r.trace, s.trace))                                  AS trace           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           , MAX(COALESCE(r.conn_max_idletime_milliseconds, NULL)) AS conn_max_idletime_milliseconds
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_sql') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_sql') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 12, 13
     )
SELECT *
FROM server_sql_details