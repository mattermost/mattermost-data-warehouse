{{config({
    "materialized": 'incremental',
    "unique_key": 'id',
    "schema": "events"
  })
}}

WITH min_active              AS (
    SELECT
        user_id
      , server_id
      , min(date) AS min_active_date
      , MAX(CASE WHEN length(user_id) < 36 then CURRENT_DATE - INTERVAL '1 DAY' ELSE DATE END) AS MAX_ACTIVE_DATE 
    FROM {{ ref('user_events_by_date') }}
    GROUP BY 1, 2),

     dates                   AS (
         SELECT
             d.date
           , m.user_id
           , m.server_id
         FROM {{ source('util', 'dates') }}      d
              JOIN min_active m
                   ON d.date >= m.min_active_date
                       AND d.date <= m.max_active_date
         GROUP BY 1, 2, 3
     ),
     events                  AS (
         SELECT
             d.date
           , d.user_id
           , MAX(d.server_id)                                                                  AS server_id 
           , MAX(e.system_admin)                                                               AS system_admin
           , MAX(e.system_user)                                                                AS system_user
           , coalesce(sum(e.total_events), 0)                                                  AS total_events
           , coalesce(sum(e.desktop_events), 0)                                                AS desktop_events
           , coalesce(sum(e.web_app_events), 0)                                                AS web_app_events
           , coalesce(sum(e.mobile_events), 0)                                                 AS mobile_events
           , sum(CASE WHEN r.event_category = 'action' THEN e.total_events ELSE 0 END)         AS action_events
           , sum(CASE WHEN r.event_category = 'api' THEN e.total_events ELSE 0 END)            AS api_events
           , sum(CASE WHEN r.event_category = 'gfycat' THEN e.total_events ELSE 0 END)         AS gfycat_events
           , sum(CASE WHEN r.event_category = 'performance' THEN e.total_events ELSE 0 END)    AS performance_events
           , sum(CASE WHEN r.event_category = 'plugins' THEN e.total_events ELSE 0 END)        AS plugins_events
           , sum(CASE WHEN r.event_category = 'settings' THEN e.total_events ELSE 0 END)       AS settings_events
           , sum(CASE WHEN r.event_category = 'signup' THEN e.total_events ELSE 0 END)         AS signup_events
           , sum(CASE WHEN r.event_category = 'system_console' THEN e.total_events ELSE 0 END) AS system_console_events
           , sum(CASE WHEN r.event_category = 'tutorial' THEN e.total_events ELSE 0 END)       AS tutorial_events
           , sum(CASE WHEN r.event_category = 'ui' THEN e.total_events ELSE 0 END)             AS ui_events
           , MAX(max_timestamp)                                                               AS max_timestamp
         FROM dates                                d
              LEFT JOIN {{ ref('user_events_by_date') }} e
                        ON d.user_id = e.user_id
                            AND COALESCE(d.server_id,'') = COALESCE(e.server_id, '')
                            AND d.date = e.date
         GROUP BY 1, 2),

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
           , coalesce(SUM(e1.mobile_events)
                          OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW ),
                      0)                                                                                           AS mobile_events_last_30_days
           , coalesce(SUM(e1.mobile_events)
                          OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ),
                      0)                                                                                           AS mobile_events_alltime
           , coalesce(MAX(e1.mobile_events)
                          OVER (PARTITION BY e1.user_id ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ),
                      0)                                                                                           AS max_mobile_events
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
           , MAX(e1.server_id)                                             AS server_id
           , MAX(e1.system_admin)                                          AS system_admin
           , MAX(e1.system_user)                                           AS system_user
           , MAX(CASE WHEN e1.total_events > 0 THEN TRUE ELSE FALSE END)   AS active
           , SUM(e1.total_events)                                          AS total_events
           , SUM(e1.desktop_events)                                        AS desktop_events
           , SUM(e1.web_app_events)                                        AS web_app_events
           , SUM(e1.mobile_events)                                         AS mobile_events
           , SUM(e1.action_events)                                         AS action_events
           , SUM(e1.api_events)                                            AS api_events
           , SUM(e1.gfycat_events)                                         AS gfycat_events
           , SUM(e1.performance_events)                                    AS performance_events
           , SUM(e1.plugins_events)                                        AS plugin_events
           , SUM(e1.settings_events)                                       AS settings_events
           , SUM(e1.signup_events)                                         AS signup_events
           , SUM(e1.system_console_events)                                 AS system_console_events
           , SUM(e1.tutorial_events)                                       AS tutorial_events
           , SUM(e1.ui_events)                                             AS ui_events
           , CASE WHEN MAX(CASE WHEN m.mau_segment = 'Current MAU' THEN 1
                            WHEN m.mau_segment = 'Reengaged MAU' THEN 2
                            WHEN m.mau_segment = 'First Time MAU' THEN 3
                            WHEN m.mau_segment = 'Newly Disengaged' THEN 4
                            ELSE 5 END) = 1 THEN 'Current MAU'
                  WHEN MAX(CASE WHEN m.mau_segment = 'Current MAU' THEN 1
                            WHEN m.mau_segment = 'Reengaged MAU' THEN 2
                            WHEN m.mau_segment = 'First Time MAU' THEN 3
                            WHEN m.mau_segment = 'Newly Disengaged' THEN 4
                            ELSE 5 END) = 2 THEN 'Reengaged MAU'
                  WHEN MAX(CASE WHEN m.mau_segment = 'Current MAU' THEN 1
                            WHEN m.mau_segment = 'Reengaged MAU' THEN 2
                            WHEN m.mau_segment = 'First Time MAU' THEN 3
                            WHEN m.mau_segment = 'Newly Disengaged' THEN 4
                            ELSE 5 END) = 3 THEN 'First Time MAU'
                  WHEN MAX(CASE WHEN m.mau_segment = 'Current MAU' THEN 1
                            WHEN m.mau_segment = 'Reengaged MAU' THEN 2
                            WHEN m.mau_segment = 'First Time MAU' THEN 3
                            WHEN m.mau_segment = 'Newly Disengaged' THEN 4
                            ELSE 5 END) = 4 THEN 'Newly Disengaged'
                  ELSE 'Disengaged' END                                AS mau_segment
           , CASE WHEN 
                CASE WHEN MAX(CASE WHEN m.mau_segment = 'Current MAU' THEN 1
                                    WHEN m.mau_segment = 'Reengaged MAU' THEN 2
                                    WHEN m.mau_segment = 'First Time MAU' THEN 3
                                    WHEN m.mau_segment = 'Newly Disengaged' THEN 4
                                    ELSE 5 END) = 1 THEN 'Current MAU'
                        WHEN MAX(CASE WHEN m.mau_segment = 'Current MAU' THEN 1
                                    WHEN m.mau_segment = 'Reengaged MAU' THEN 2
                                    WHEN m.mau_segment = 'First Time MAU' THEN 3
                                    WHEN m.mau_segment = 'Newly Disengaged' THEN 4
                                    ELSE 5 END) = 2 THEN 'Reengaged MAU'
                        WHEN MAX(CASE WHEN m.mau_segment = 'Current MAU' THEN 1
                                    WHEN m.mau_segment = 'Reengaged MAU' THEN 2
                                    WHEN m.mau_segment = 'First Time MAU' THEN 3
                                    WHEN m.mau_segment = 'Newly Disengaged' THEN 4
                                    ELSE 5 END) = 3 THEN 'First Time MAU'
                        WHEN MAX(CASE WHEN m.mau_segment = 'Current MAU' THEN 1
                                    WHEN m.mau_segment = 'Reengaged MAU' THEN 2
                                    WHEN m.mau_segment = 'First Time MAU' THEN 3
                                    WHEN m.mau_segment = 'Newly Disengaged' THEN 4
                                    ELSE 5 END) = 4 THEN 'Newly Disengaged'
                        ELSE 'Disengaged' END IN ('First Time MAU', 'Reengaged MAU', 'Current MAU') THEN TRUE
                  ELSE FALSE END                                       AS mau
           , MIN(m.min_active_date)                                         AS first_active_date
           , MAX(m.last_active_date)                                   AS last_active_date
           , MAX(m.events_last_30_days)                                AS events_last_30_days
           , MAX(m.events_last_31_days)                                AS events_last_31_days
           , MAX(m.events_alltime)                                     AS events_alltime
           , MAX(m.max_events)                                         AS max_events
           , MAX(m.mobile_events_last_30_days)                         AS mobile_events_last_30_days
           , MAX(m.mobile_events_alltime)                              AS mobile_events_alltime
           , MAX(m.max_mobile_events)                                  AS max_mobile_events
           , MAX(e1.max_timestamp)                                     AS max_timestamp
           , {{ dbt_utils.surrogate_key('e1.date', 'e1.user_id') }}    AS id
         FROM events   e1
              JOIN mau m
                   ON e1.user_id = m.user_id
                       AND e1.date = m.date
         WHERE e1.date <= CURRENT_DATE - interval '1 day'
         {% if is_incremental() %}

         AND e1.date >= (SELECT MAX(date) FROM {{this}})

         {% endif %}
         GROUP BY 1, 2, 33)
SELECT *
FROM user_events_by_date_agg