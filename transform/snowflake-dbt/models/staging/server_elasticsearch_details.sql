{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , max(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_elasticsearch') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_elasticsearch_details AS (
         SELECT
             timestamp::DATE                        AS date
           , e.user_id                              AS server_id
           , max(bulk_indexing_time_window_seconds) AS bulk_indexing_time_window_seconds
           , max(channel_index_replicas)            AS channel_index_replicas
           , max(request_timeout_seconds)           AS request_timeout_seconds
           , max(live_indexing_batch_size)          AS live_indexing_batch_size
           , max(post_index_shards)                 AS post_index_shards
           , max(channel_index_shards)              AS channel_index_shards
           , max(enable_indexing)                   AS enable_indexing
           , max(skip_tls_verification)             AS skip_tls_verification
           , max(isdefault_username)                AS isdefault_username
           , max(isdefault_connection_url)          AS isdefault_connection_url
           , max(isdefault_password)                AS isdefault_password
           , max(sniff)                             AS sniff
           , max(enable_autocomplete)               AS enable_autocomplete
           , max(isdefault_index_prefix)            AS isdefault_index_prefix
           , max(user_index_replicas)               AS user_index_replicas
           , max(user_index_shards)                 AS user_index_shards
           , max(post_index_replicas)               AS post_index_replicas
         FROM {{ source('staging_config', 'config_elasticsearch') }} e
              JOIN max_timestamp               mt
                   ON e.user_id = mt.user_id
                       AND mt.max_timestamp = e.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_elasticsearch_details