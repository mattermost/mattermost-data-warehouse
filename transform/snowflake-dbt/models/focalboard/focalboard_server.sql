{{config({
    "materialized": "incremental",
    "schema": "focalboard",
    "tags":"hourly",
    "unique_key":"id"
  })
}}

WITH max_time AS (
  SELECT 
    user_id
  , timestamp::date as date
  , MAX(TIMESTAMP) AS max_time
  FROM {{ source('hacktoberboard_prod', 'server') }}
  WHERE TIMESTAMP::DATE <= CURRENT_DATE
  GROUP BY 1, 2
), 

focalboard_server AS (
    SELECT 
        server.timestamp::date as logging_date
        , server.version
        , server.context_ip
        , server.event
        , server.build_number
        , server.build_hash
        , server.context_library_version
        , server.edition
        , server.event_text
        , server.anonymous_id
        , server.timestamp
        , server.context_request_ip
        , server.original_timestamp
        , server.sent_at
        , server.user_id
        , server.operating_system
        , server.uuid_ts
        , server.context_library_name
        , server.received_at
      , {{ dbt_utils.surrogate_key(['server.timestamp::date', 'server.user_id'])}} as id
    FROM {{ source('hacktoberboard_prod', 'server') }} server
    JOIN max_time mt
      ON server.user_id = mt.user_id
      AND server.timestamp = mt.max_time
    WHERE server.TIMESTAMP::DATE <= CURRENT_DATE
    {% if is_incremental() %}
      and server.timestamp::date >= (select max(logging_date) from {{ this }})
    {% endif %}
)

SELECT *
FROM focalboard_server