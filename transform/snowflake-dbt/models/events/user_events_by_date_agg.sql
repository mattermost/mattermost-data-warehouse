{{config({
    "materialized": 'incremental',
    "schema": "events"
  })
}}

WITH min_active              AS (
    SELECT
        user_id
      , min(date) AS min_active_date
    FROM {{ ref('user_events_by_date') }}
    GROUP BY 1),

     dates                   AS (
         SELECT
             d.date
           , m.user_id
         FROM {{ source('utils', 'dates') }}      d
              JOIN min_active m
                   ON d.date >= m.min_active_date
                       AND d.date < current_date
     ),
     events                  AS (
         SELECT
             d.date
           , d.user_id
           , e.system_admin
           , e.system_user
           , CASE WHEN sum(e.num_events) > 0 THEN TRUE ELSE FALSE END                        AS active
           , coalesce(sum(e.num_events), 0)                                                  AS total_events
           , sum(CASE WHEN r.event_category = 'action' THEN e.num_events ELSE 0 END)         AS action_events
           , sum(CASE WHEN r.event_category = 'api' THEN e.num_events ELSE 0 END)            AS api_events
           , sum(CASE WHEN r.event_category = 'gfycat' THEN e.num_events ELSE 0 END)         AS gfycat_events
           , sum(CASE WHEN r.event_category = 'performance' THEN e.num_events ELSE 0 END)    AS performance_events
           , sum(CASE WHEN r.event_category = 'plugins' THEN e.num_events ELSE 0 END)        AS plugins_events
           , sum(CASE WHEN r.event_category = 'settings' THEN e.num_events ELSE 0 END)       AS settings_events
           , sum(CASE WHEN r.event_category = 'signup' THEN e.num_events ELSE 0 END)         AS signup_events
           , sum(CASE WHEN r.event_category = 'system_console' THEN e.num_events ELSE 0 END) AS system_console_events
           , sum(CASE WHEN r.event_category = 'tutorial' THEN e.num_events ELSE 0 END)       AS tutorial_events
           , sum(CASE WHEN r.event_category = 'ui' THEN e.num_events ELSE 0 END)             AS ui_events
         FROM dates                                d
              LEFT JOIN {{ ref('user_events_by_date') }} e
                        ON d.user_id = e.user_id
                            AND d.date = e.date
              LEFT JOIN {{ ref('events_registry') }}     r
                        ON e.uuid = r.uuid
         GROUP BY 1, 2, 3, 4),

     mau                     AS (
         SELECT
             e1.user_id
           , e1.date
           , m.min_active_date
           , count(DISTINCT e2.date)           AS date_count
           , CASE WHEN min(m.min_active_date) = e1.date AND sum(e1.total_events) > 0 THEN 'First Time MAU'
                  WHEN sum(CASE WHEN e2.date < e1.date AND e2.date >= e1.date - INTERVAL '30 days' THEN e2.total_events
                                ELSE 0 END) = 0
                      AND sum(CASE WHEN e2.date = e1.date THEN e2.total_events ELSE 0 END) > 0
                                                                                     THEN 'Reengaged MAU'
                  WHEN sum(CASE WHEN e2.date >= e1.date - INTERVAL '30 days' AND e2.date <= e1.date THEN e2.total_events
                                ELSE 0 END) > 0
                                                                                     THEN 'Current MAU'
                  WHEN sum(CASE WHEN e2.date = e1.date - INTERVAL '31 days' THEN e2.total_events ELSE 0 END) > 0
                                                                                     THEN 'Newly Disengaged'
                  ELSE 'Disengaged' END        AS mau_segment
           , coalesce(sum(CASE WHEN e2.date >= e1.date - INTERVAL '30 days' AND e2.date <= e1.date THEN e2.total_events
                               ELSE 0 END), 0) AS events_last_30_days
           , coalesce(sum(e2.total_events), 0) AS events_last_31_days
         FROM events           e1
              JOIN min_active  m
                   ON e1.user_id = m.user_id
              LEFT JOIN events e2
                        ON e1.user_id = e2.user_id
                            AND e2.date <= e1.date
                            AND e2.date >= e1.date - INTERVAL '31 days'
         GROUP BY 1, 2, 3
     ),
     user_events_by_date_agg AS (
         SELECT
             e1.date
           , e1.user_id
           , e1.system_admin
           , e1.system_user
           , e1.active
           , e1.total_events
           , e1.action_events
           , e1.api_events
           , e1.gfycat_events
           , e1.performance_events
           , e1.plugins_events
           , e1.settings_events
           , e1.signup_events
           , e1.system_console_events
           , e1.tutorial_events
           , e1.ui_events
           , m.mau_segment
           , CASE WHEN m.mau_segment IN ('First Time MAU', 'Reengaged MAU', 'Current MAU') THEN TRUE
                  ELSE FALSE END                                 AS mau
           , m.min_active_date                                   AS first_active_date
           , MAX(CASE WHEN e2.active THEN e2.date ELSE NULL END) AS last_active_date
           , m.events_last_30_days
           , m.events_last_31_days
           , SUM(e2.total_events)                                AS events_alltime
           , MAX(e2.total_events)                                AS max_events
         FROM events           e1
              JOIN mau         m
                   ON e1.user_id = m.user_id
                       AND e1.date = m.date
              LEFT JOIN events e2
                        ON e1.user_id = e2.user_id
                            AND e2.date <= e1.date
         {% if is_incremental() %}

         WHERE e1.date >= (SELECT MAX(date) FROM {{this}})
         
         {% endif %}
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22)
SELECT *
FROM user_events_by_date_agg