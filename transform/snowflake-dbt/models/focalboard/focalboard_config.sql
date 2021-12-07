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
  FROM {{ source('hacktoberboard_prod', 'config') }}
  WHERE received_at::DATE <= CURRENT_DATE
  GROUP BY 1, 2
), 

focalboard_config AS (
    SELECT      
    {% if not is_incremental() %}
    DISTINCT
    {% endif %}
        config.received_at::date as logging_date
        , config.context_request_ip
        , config.timestamp
        , config.anonymous_id
        , config.port
        , config.sent_at
        , config.user_id
        , config.usessl
        , config.received_at
        , config.context_library_version
        , config.dbtype
        , config.serverroot
        , config.original_timestamp
        , config.context_ip
        , config.single_user
        , config.event
        , config.event_text
        , config.uuid_ts
      , {{ dbt_utils.surrogate_key(['config.received_at::date', 'config.user_id'])}} as id
    FROM {{ source('hacktoberboard_prod', 'config') }} config
    JOIN max_time mt
      ON config.user_id = mt.user_id
      AND config.received_at = mt.max_time
      AND config.received_at = mt.max_ts
    WHERE config.received_at::DATE <= CURRENT_DATE
    {% if is_incremental() %}
      and config.received_at > (select max(received_at) from {{ this }})
      {%- if col_count != none -%}

      {{dbt_utils.group_by(n=col_count)}}

      {%- endif -%}
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY config.anonymous_id ORDER BY config.timestamp) = 1
)

SELECT *
FROM focalboard_config