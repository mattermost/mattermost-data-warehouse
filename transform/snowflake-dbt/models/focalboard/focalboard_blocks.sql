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
  FROM {{ source('hacktoberboard_prod', 'blocks') }}
  WHERE TIMESTAMP::DATE <= CURRENT_DATE
  GROUP BY 1, 2
), 

focalboard_blocks AS (
    SELECT 
        blocks.timestamp::date as logging_date
        , blocks.context_request_ip
        , blocks.comment
        , blocks.divider
        , blocks.id
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
      , {{ dbt_utils.surrogate_key(['blocks.timestamp::date', 'blocks.user_id'])}} as id
    FROM {{ source('hacktoberboard_prod', 'blocks') }} blocks
    JOIN max_time mt
      ON blocks.user_id = mt.user_id
      AND blocks.timestamp = mt.max_time
    WHERE blocks.TIMESTAMP::DATE <= CURRENT_DATE
    {% if is_incremental() %}
      and blocks.timestamp::date >= (select max(logging_date) from {{ this }})
    {% endif %}
)

SELECT *
FROM focalboard_blocks 