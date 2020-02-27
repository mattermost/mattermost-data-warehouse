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
         FROM {{ source('util', 'dates') }}      d
              JOIN min_active m
                   ON d.date >= m.min_active_date
                       AND d.date < current_date
         GROUP BY 1, 2
     ),
     events                  AS (
         SELECT
             d.date
           , d.user_id
           , e.server_id
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
         GROUP BY 1, 2, 3, 4, 5),

     mau                     AS (
         SELECT
             e1.user_id
           , e1.date
           , m.min_active_date
           , CASE WHEN m.min_active_date = e1.date AND e1.total_events > 0 THEN 'First Time MAU'
                  WHEN SUM(e1.total_events)
                           OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) = 0
                      AND e1.total_events > 0                              THEN 'Reengaged MAU'
                  WHEN SUM(e1.total_events)
                           OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) > 0
                                                                           THEN 'Current MAU'
                  WHEN sum(e1.total_events)
                           OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN 31 PRECEDING AND 31 PRECEDING) >
                       0                                                   THEN 'Newly Disengaged'
                  ELSE 'Disengaged' END                                                                            AS mau_segment
           , coalesce(SUM(e1.total_events)
                          OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW ),
                      0)                                                                                           AS events_last_30_days
           , coalesce(sum(e1.total_events)
                          OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN 31 PRECEDING AND CURRENT ROW ),
                      0)                                                                                           AS events_last_31_days
           , coalesce(SUM(e1.total_events)
                          OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ),
                      0)                                                                                           AS events_alltime
           , coalesce(MAX(e1.total_events)
                          OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ),
                      0)                                                                                           AS max_events
           , MAX(CASE WHEN e1.total_events > 0 THEN e1.date ELSE NULL END)
                 OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS last_active_date
         FROM events          e1
              JOIN min_active m
                   ON e1.user_id = m.user_id
     ),
     user_events_by_date_agg AS (
         SELECT
             e1.date
           , e1.user_id
           , e1.server_id
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
                  ELSE FALSE END     AS mau
           , m.min_active_date       AS first_active_date
           , MAX(m.last_active_date) AS last_active_date
           , m.events_last_30_days
           , m.events_last_31_days
           , SUM(m.events_alltime)   AS events_alltime
           , MAX(m.max_events)       AS max_events
         FROM events   e1
              JOIN mau m
                   ON e1.user_id = m.user_id
                       AND e1.date = m.date
         {% if is_incremental() %}

         WHERE e1.date >= (SELECT MAX(date) FROM {{this}})

         {% endif %}
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23)
SELECT *
FROM user_events_by_date_agg