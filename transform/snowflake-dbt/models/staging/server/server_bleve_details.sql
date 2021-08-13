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
    FROM {{ source('mattermost2', 'config_bleve') }}
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
    FROM {{ source('mm_telemetry_prod', 'config_bleve') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_bleve_details AS (
         SELECT
              COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
            , COALESCE(r.user_id, s.user_id)                                        AS server_id
            , MAX(COALESCE(r.BULK_INDEXING_TIME_WINDOW_SECONDS, s.BULK_INDEXING_TIME_WINDOW_SECONDS)) AS BULK_INDEXING_TIME_WINDOW_SECONDS
            , MAX(COALESCE(r.ENABLE_AUTOCOMPLETE, s.ENABLE_AUTOCOMPLETE)) AS ENABLE_AUTOCOMPLETE
            , MAX(COALESCE(r.ENABLE_INDEXING, s.ENABLE_INDEXING)) AS ENABLE_INDEXING
            , MAX(COALESCE(r.ENABLE_SEARCHING, s.ENABLE_SEARCHING)) AS ENABLE_SEARCHING
            , {{ dbt_utils.surrogate_key(['COALESCE(r.timestamp::DATE, s.timestamp::date)', 'COALESCE(r.user_id, s.user_id)']) }} AS id
            , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_bleve') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_bleve') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 7, 8
     )
SELECT *
FROM server_bleve_details