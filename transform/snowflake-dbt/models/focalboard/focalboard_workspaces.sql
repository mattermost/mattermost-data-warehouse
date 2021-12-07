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
  FROM {{ source('hacktoberboard_prod', 'workspaces') }}
  WHERE received_at::DATE <= CURRENT_DATE
  GROUP BY 1, 2
),

focalboard_workspaces AS (
    SELECT
    {% if not is_incremental() %}
    DISTINCT
    {% endif %}
        workspaces.received_at::date as logging_date
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
      , {{ dbt_utils.surrogate_key(['workspaces.received_at::date', 'workspaces.user_id'])}} as id
    FROM {{ source('hacktoberboard_prod', 'workspaces') }} workspaces
    JOIN max_time mt
      ON workspaces.user_id = mt.user_id
      AND workspaces.received_at = mt.max_time
      AND workspaces.received_at = mt.max_ts
    WHERE workspaces.received_at::DATE <= CURRENT_DATE
    {% if is_incremental() %}
      and workspaces.received_at > (select max(received_at) from {{ this }})
      {%- if col_count != none -%}

      {{dbt_utils.group_by(n=col_count)}}

      {%- endif -%}
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workspaces.anonymous_id ORDER BY workspaces.timestamp) = 1
)

SELECT *
FROM focalboard_workspaces 