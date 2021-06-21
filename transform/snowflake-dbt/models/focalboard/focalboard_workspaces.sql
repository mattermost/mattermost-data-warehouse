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
  FROM {{ source('hacktoberboard_prod', 'workspaces') }}
  WHERE TIMESTAMP::DATE <= CURRENT_DATE
  GROUP BY 1, 2
),

focalboard_workspaces AS (
    SELECT 
        workspaces.timestamp::date as logging_date
        , workspaces.original_timestamp
        , workspaces.workspaces
        , workspaces.uuid_ts
        , workspaces.context_request_ip
        , workspaces.context_library_version
        , workspaces.received_at
        , workspaces.event
        , workspaces.context_ip
        , workspaces.context_library_name
        , workspaces.timestamp
        , workspaces.event_text
        , workspaces.anonymous_id
        , workspaces.sent_at
        , workspaces.user_id
      , {{ dbt_utils.surrogate_key(['workspaces.timestamp::date', 'workspaces.user_id'])}} as id
    FROM {{ source('hacktoberboard_prod', 'workspaces') }} workspaces
    JOIN max_time mt
      ON workspaces.user_id = mt.user_id
      AND workspaces.timestamp = mt.max_time
    WHERE workspaces.TIMESTAMP::DATE <= CURRENT_DATE
    {% if is_incremental() %}
      and workspaces.timestamp::date >= (select max(logging_date) from {{ this }})
    {% endif %}
)

SELECT *
FROM focalboard_workspaces 