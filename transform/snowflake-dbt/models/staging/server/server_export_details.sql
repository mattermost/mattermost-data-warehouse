{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}


WITH max_export_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id AS server_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_export') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(timestamp::date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

server_export_details AS (
    SELECT 
          r.timestamp::DATE                      AS date
        , r.user_id                                        AS server_id
        , MAX(r.CONTEXT_REQUEST_IP) AS context_request_ip
        , MAX(r.ANONYMOUS_ID) AS anonymous_id
        , MAX(r.CONTEXT_IP) AS context_ip
        , MAX(r.CONTEXT_TRAITS_INSTALLATIONID) AS context_traits_installationid
        , MAX(r.SENT_AT) AS sent_at
        , MAX(r.ORIGINAL_TIMESTAMP) AS original_timestamp
        , MAX(r.RETENTION_DAYS) AS retention_days
        , MAX(r.UUID_TS) AS uuid_ts
        , MAX(r.RECEIVED_AT) AS received_at
        , MAX(r.EVENT_TEXT) AS event_text
        , MAX(r.TIMESTAMP) AS timestamp
        , MAX(r.EVENT) AS event
        , MAX(r.CONTEXT_LIBRARY_NAME) AS context_library_name
        , MAX(r.CONTEXT_LIBRARY_VERSION) AS context_library_version
        , MAX(r.CONTEXT_TRAITS_INSTALLATION_ID) AS context_traits_installation_id
        , {{ dbt_utils.surrogate_key(['r.timestamp::date', 'r.user_id']) }} AS id
    FROM {{ source('mm_telemetry_prod', 'config_export') }} r
    JOIN max_export_timestamp        mt
                   ON r.user_id = mt.server_id
                       AND mt.max_timestamp = r.timestamp
    {{dbt_utils.group_by(n=2)}}
)

SELECT *
FROM server_export_details