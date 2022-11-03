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
  FROM {{ source('hacktoberboard_prod', 'blocks') }}
  WHERE received_at::DATE <= CURRENT_DATE
  GROUP BY 1, 2
), 

focalboard_blocks AS (
    SELECT      
    {% if not is_incremental() %}
    DISTINCT
    {% endif %}
        blocks.received_at::date as logging_date
        , blocks.context_request_ip
        , blocks.comment
        , blocks.divider
        , blocks.received_at
        , blocks.sent_at
        , blocks.anonymous_id
        , blocks.text
        , blocks.context_library_version
        , blocks.context_ip
        , blocks.board
        , blocks._view
        , blocks.uuid_ts
        , blocks.event
        , blocks.image
        , blocks.context_library_name
        , blocks.event_text
        , blocks.card
        , blocks.original_timestamp
        , blocks.user_id
        , blocks.checkbox
        , blocks.timestamp
      , {{ dbt_utils.surrogate_key(['blocks.received_at::date', 'blocks.user_id'])}} as id
    FROM {{ source('hacktoberboard_prod', 'blocks') }} blocks
    JOIN max_time mt
      ON blocks.user_id = mt.user_id
      AND blocks.received_at = mt.max_time
      AND blocks.received_at = mt.max_ts
    WHERE blocks.received_at::DATE <= CURRENT_DATE
    {% if is_incremental() %}
      and blocks.received_at > (select max(received_at) from {{ this }})
      {%- if col_count != none -%}

      {{dbt_utils.group_by(n=col_count)}}

      {%- endif -%}
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY blocks.anonymous_id ORDER BY blocks.timestamp) = 1
)

SELECT *
FROM focalboard_blocks 