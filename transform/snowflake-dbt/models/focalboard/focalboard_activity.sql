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
  FROM {{ source('hacktoberboard_prod', 'activity') }}
  WHERE TIMESTAMP::DATE <= CURRENT_DATE
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
      , activity.id
      , activity.original_timestamp
      , activity.context_library_version
      , activity.timestamp
      , activity.context_ip
      , {{ dbt_utils.surrogate_key(['activity.timestamp::date', 'activity.user_id'])}}  as id
    FROM {{ source('hacktoberboard_prod', 'activity') }} activity
    JOIN max_time mt
      ON activity.user_id = mt.user_id
      AND activity.timestamp = mt.max_time
    WHERE activity.TIMESTAMP::DATE <= CURRENT_DATE
    {% if is_incremental() %}
      and activity.timestamp::date >= (select max(logging_date) from {{ this }})
    {% endif %}
)

SELECT *
FROM focalboard_activity