{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_timestamp          AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_cluster') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_cluster_details AS (
         SELECT
             timestamp::DATE              AS date
           , cc.user_id                   AS server_id
           , MAX(advertise_address)       AS advertise_address
           , MAX(bind_address)            AS bind_address
           , MAX(enable)                  AS enable_cluster
           , MAX(network_interface)       AS network_interface
           , MAX(read_only_config)        AS read_only_config
           , MAX(use_experimental_gossip) AS use_experimental_gossip
           , MAX(use_ip_address)          AS use_ip_address, {{ dbt_utils.surrogate_key('timestamp::date', 'cc.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_cluster') }} cc
              JOIN max_timestamp         mt
                   ON cc.user_id = mt.user_id
                       AND mt.max_timestamp = cc.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_cluster_details