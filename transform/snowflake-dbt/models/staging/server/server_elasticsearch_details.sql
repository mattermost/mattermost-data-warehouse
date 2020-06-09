{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_elasticsearch') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_elasticsearch_details AS (
         SELECT
             timestamp::DATE                        AS date
           , e.user_id                              AS server_id
           , MAX(bulk_indexing_time_window_seconds) AS bulk_indexing_time_window_seconds
           , MAX(channel_index_replicas)            AS channel_index_replicas
           , MAX(channel_index_shards)              AS channel_index_shards
           , MAX(enable_autocomplete)               AS enable_autocomplete
           , MAX(enable_indexing)                   AS enable_indexing
           , MAX(enable_searching)                  AS enable_searching
           , MAX(isdefault_connection_url)          AS isdefault_connection_url
           , MAX(isdefault_index_prefix)            AS isdefault_index_prefix
           , MAX(isdefault_password)                AS isdefault_password
           , MAX(isdefault_username)                AS isdefault_username
           , MAX(live_indexing_batch_size)          AS live_indexing_batch_size
           , MAX(post_index_replicas)               AS post_index_replicas
           , MAX(post_index_shards)                 AS post_index_shards
           , MAX(request_timeout_seconds)           AS request_timeout_seconds
           , MAX(skip_tls_verification)             AS skip_tls_verification
           , MAX(sniff)                             AS sniff
           , MAX(trace)                             AS trace
           , MAX(user_index_replicas)               AS user_index_replicas
           , MAX(user_index_shards)                 AS user_index_shards
           , {{ dbt_utils.surrogate_key('timestamp::date', 'e.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_elasticsearch') }} e
              JOIN max_timestamp               mt
                   ON e.user_id = mt.user_id
                       AND mt.max_timestamp = e.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_elasticsearch_details