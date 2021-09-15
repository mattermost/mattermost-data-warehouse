{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_elasticsearch') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_elasticsearch') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

     server_elasticsearch_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::date)              AS date
           , COALESCE(s.user_id, r.user_id)                              AS server_id
           , MAX(COALESCE(s.bulk_indexing_time_window_seconds, r.bulk_indexing_time_window_seconds)) AS bulk_indexing_time_window_seconds
           , MAX(COALESCE(s.channel_index_replicas, r.channel_index_replicas))            AS channel_index_replicas
           , MAX(COALESCE(s.channel_index_shards, r.channel_index_shards))              AS channel_index_shards
           , MAX(COALESCE(s.enable_autocomplete, r.enable_autocomplete))               AS enable_autocomplete
           , MAX(COALESCE(s.enable_indexing, r.enable_indexing))                   AS enable_indexing
           , MAX(COALESCE(s.enable_searching, r.enable_searching))                  AS enable_searching
           , MAX(COALESCE(s.isdefault_connection_url, r.isdefault_connection_url))          AS isdefault_connection_url
           , MAX(COALESCE(s.isdefault_index_prefix, r.isdefault_index_prefix))            AS isdefault_index_prefix
           , MAX(COALESCE(s.isdefault_password, r.isdefault_password))                AS isdefault_password
           , MAX(COALESCE(s.isdefault_username, r.isdefault_username))                AS isdefault_username
           , MAX(COALESCE(s.live_indexing_batch_size, r.live_indexing_batch_size))          AS live_indexing_batch_size
           , MAX(COALESCE(s.post_index_replicas, r.post_index_replicas))               AS post_index_replicas
           , MAX(COALESCE(s.post_index_shards, r.post_index_shards))                 AS post_index_shards
           , MAX(COALESCE(s.request_timeout_seconds, r.request_timeout_seconds))           AS request_timeout_seconds
           , MAX(COALESCE(s.skip_tls_verification, r.skip_tls_verification))             AS skip_tls_verification
           , MAX(COALESCE(s.sniff, r.sniff))                             AS sniff
           , MAX(COALESCE(s.trace, r.trace))                             AS trace
           , MAX(COALESCE(s.user_index_replicas, r.user_index_replicas))               AS user_index_replicas
           , MAX(COALESCE(s.user_index_shards, r.user_index_shards))                 AS user_index_shards           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_elasticsearch') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_elasticsearch') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 22, 23
     )
SELECT *
FROM server_elasticsearch_details