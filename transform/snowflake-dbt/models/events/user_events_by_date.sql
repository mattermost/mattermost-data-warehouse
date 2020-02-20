{{config({
    "materialized": 'incremental',
    "schema": "mattermost"
  })
}}

WITH user_events_by_date AS (
    SELECT
        e.timestamp::DATE                                                                      AS date
      , e.user_id                                                                              AS server_id
      , e.user_actual_id                                                                       AS user_id
      , MIN(e.user_actual_role)                                                                AS user_role
      , r.uuid
      , LOWER(e.type)                                                                          AS event_name
      , COUNT(*)                                                                               AS num_events
    FROM {{ source('mattermost2', 'event')}} e
    LEFT JOIN {{ ref('events_registry') }} r
              ON LOWER(e.type) = r.event_name
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
    WHERE timestamp::DATE > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    AND user_actual_id IS NOT NULL
    GROUP BY 1, 2, 3, 5, 6)
SELECT *
FROM user_events_by_date