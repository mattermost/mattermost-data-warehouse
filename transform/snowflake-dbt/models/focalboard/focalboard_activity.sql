{{config({
    "materialized": "incremental",
    "schema": "focalboard",
    "tags":"hourly",
    "unique_key":"id"
  })
}}

{% set col_count = get_rudder_track_column_count() %}

WITH max_time AS (
  SELECT 
    user_id
  , timestamp::date as date
  , MAX(received_at) AS max_time
  FROM {{ source('hacktoberboard_prod', 'activity') }}
  WHERE received_at::DATE <= CURRENT_DATE
  GROUP BY 1, 2
), 

focalboard_activity AS (
    SELECT 
        activity.timestamp::date as logging_date
      , activity.received_at
      , activity.weekly_active_users
      , activity.sent_at
      , activity.monthly_active_users
      , activity.anonymous_id
      , activity.event
      , activity.user_id
      , activity.uuid_ts
      , activity.registered_users
      , activity.context_request_ip
      , activity.daily_active_users
      , activity.event_text
      , activity.context_library_name
      , activity.original_timestamp
      , activity.context_library_version
      , activity.timestamp
      , activity.context_ip
      , {{ dbt_utils.surrogate_key(['activity.timestamp::date', 'activity.user_id'])}}  as id
    FROM {{ source('hacktoberboard_prod', 'activity') }} activity
    JOIN max_time mt
      ON activity.user_id = mt.user_id
      AND activity.received_at = mt.max_time
    WHERE activity.received_at::DATE <= CURRENT_DATE
    {% if is_incremental() %}
      and activity.received_at >= (select max(received_at) from {{ this }})
      {%- if col_count != none -%}

      {{dbt_utils.group_by(n=col_count)}}

      {%- endif -%}
    {% endif %}
)

SELECT *
FROM focalboard_activity