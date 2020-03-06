{{config({
    "materialized": 'incremental',
    "schema": "events"
  })
}}

WITH mobile_events       AS (
    SELECT
        m.timestamp::DATE       AS date
      , m.user_id               AS server_id
      , m.user_actual_id        AS user_id
      , min(m.user_actual_role) AS user_role
      , LOWER(m.type)           AS event_name
      , 'mobile'                AS event_type
      , COUNT(*)                AS num_events
    FROM {{ source('mattermost2', 'event_mobile')}} m
    WHERE user_actual_id IS NOT NULL
    {% if is_incremental() %}

      AND timestamp::DATE > (SELECT MAX(date) from {{this}})

    {% endif %}
    GROUP BY 1, 2, 3, 5, 6
),
     events              AS (
         SELECT
             e.timestamp::DATE                                                                         AS date
           , e.user_id                                                                                 AS server_id
           , e.user_actual_id                                                                          AS user_id
           , min(e.user_actual_role)                                                                   AS user_role
           , LOWER(e.type)                                                                             AS event_name
           , CASE WHEN LOWER(e.context_user_agent) LIKE '%electron%' THEN 'desktop' ELSE 'web_app' END AS event_type
           , COUNT(*)                                                                                  AS num_events
         FROM {{ source('mattermost2', 'event') }} e
         WHERE user_actual_id IS NOT NULL
         {% if is_incremental() %}

          AND timestamp::DATE > (SELECT MAX(date) from {{this}})

         {% endif %}
         GROUP BY 1, 2, 3, 5, 6
     ),

     all_events          AS (
         SELECT *
         FROM mobile_events
         UNION ALL
         SELECT *
         FROM events
     ),

     user_events_by_date AS (
         SELECT
             e.date
           , e.server_id
           , e.user_id
           , max(e.user_role)                                                                         AS user_role
           , CASE WHEN MAX(split_part(e.user_role, ',', 1)) = 'system_admin' THEN TRUE ELSE FALSE END AS system_admin
           , CASE WHEN MAX(e.user_role) = 'system_user' THEN TRUE ELSE FALSE END                      AS system_user
           , r.event_id                                                                               AS event_id
           , e.event_name
           , sum(e.num_events)                                                                        AS total_events
           , sum(CASE WHEN e.event_type = 'desktop' THEN e.num_events ELSE 0 END)                     AS desktop_events
           , sum(CASE WHEN e.event_type = 'web_app' THEN e.num_events ELSE 0 END)                     AS web_app_events
           , sum(CASE WHEN e.event_type = 'mobile' THEN e.num_events ELSE 0 END)                      AS mobile_events
         FROM all_events                  e
              LEFT JOIN {{ ref('events_registry') }} r
                   ON e.event_name = r.event_name
         GROUP BY 1, 2, 3, 7, 8
     )
SELECT *
FROM user_events_by_date
