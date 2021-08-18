{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp          AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_cluster') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp          AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_cluster') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

     server_cluster_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::date)              AS date
           , COALESCE(s.user_id, r.user_id)                              AS server_id
           , MAX(COALESCE(s.advertise_address, r.advertise_address))       AS advertise_address
           , MAX(COALESCE(s.bind_address, r.bind_address))            AS bind_address
           , MAX(COALESCE(s.enable, r.enable))                  AS enable_cluster
           , MAX(COALESCE(s.network_interface, r.network_interface))       AS network_interface
           , MAX(COALESCE(s.read_only_config, r.read_only_config))        AS read_only_config
           , MAX(COALESCE(s.use_experimental_gossip, r.use_experimental_gossip)) AS use_experimental_gossip
           , MAX(COALESCE(s.use_ip_address, r.use_ip_address))          AS use_ip_address
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , MAX(COALESCE(r.enable_experimental_gossip_encryption, NULL)) AS enable_experimental_gossip_encryption
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           , MAX(COALESCE(r.enable_gossip_compression, NULL)) AS enable_gossip_compression
           FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_cluster') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_cluster') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 10, 12
     )
SELECT *
FROM server_cluster_details