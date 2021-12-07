{{config({
    "materialized": "incremental",
    "schema": "focalboard",
    "tags":"hourly",
    "unique_key":"id"
  })
}}

{% if is_incremental() %}
  {% set col_count = get_rudder_track_column_count() %}
{% endif %}

WITH max_time AS (
  SELECT 
    user_id
  , received_at::date as date
  , MAX(received_at) AS max_time
  , MAX(received_at) AS max_ts
  FROM {{ source('hacktoberboard_prod', 'server') }}
  WHERE received_at::DATE <= CURRENT_DATE
  GROUP BY 1, 2
), 

focalboard_server AS (
    SELECT     
    {% if not is_incremental() %}
    DISTINCT
    {% endif %} 
        server.received_at::date as logging_date
        , server.server_id
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
      , {{ dbt_utils.surrogate_key(['server.received_at::date', 'server.user_id'])}} as id
    FROM {{ source('hacktoberboard_prod', 'server') }} server
    JOIN max_time mt
      ON server.user_id = mt.user_id
      AND server.received_at = mt.max_time
      AND server.received_at = mt.max_ts
    WHERE server.received_at::DATE <= CURRENT_DATE
    {% if is_incremental() %}
      and server.received_at > (select max(received_at) from {{ this }})
      {%- if col_count != none -%}

      {{dbt_utils.group_by(n=col_count)}}

      {%- endif -%}
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY server.anonymous_id ORDER BY server.timestamp) = 1

    
)

SELECT *
FROM focalboard_server