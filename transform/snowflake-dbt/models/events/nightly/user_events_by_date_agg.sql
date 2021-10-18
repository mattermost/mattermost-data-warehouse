{{config({
    "materialized": 'incremental',
    "unique_key": 'id',
    "schema": "events"
  })
}}

WITH min_active              AS (
    SELECT
        user_actual_id as user_id
      , COALESCE(user_events_telemetry.user_id, IFF(LENGTH(user_events_telemetry.context_server) < 26, NULL, user_events_telemetry.context_server),
                          IFF(LENGTH(user_events_telemetry.context_traits_userid) < 26, NULL,user_events_telemetry.context_traits_userid),
                          IFF(LENGTH(user_events_telemetry.context_server) < 26, NULL, user_events_telemetry.context_server)) as server_id

      , MAX(user_actual_role) AS user_actual_role
      , min(received_at::date) AS min_active_date
      , CASE WHEN MAX(received_at::date) < CURRENT_DATE
            THEN CURRENT_DATE ELSE
            MAX(received_at::date) END AS MAX_ACTIVE_DATE 
    FROM {{ ref('user_events_telemetry') }}
    WHERE user_actual_id is not null
    GROUP BY 1, 2),

     dates                   AS (
         SELECT
             d.date
           , m.user_id
           , m.server_id
           , m.user_actual_role
         FROM {{ ref('dates') }}      d
              JOIN min_active m
                   ON d.date >= m.min_active_date
                       AND d.date <= m.max_active_date
         GROUP BY 1, 2, 3, 4
     ),
     events                  AS (
         SELECT
             d.date
           , d.user_id
           , d.server_id                                                                       AS server_id 
           , MAX(CASE WHEN COALESCE(e.user_actual_role, 'none') in ('system_admin', 'system_admin, system_user', 'system_administrator',
                                             'admin_user', 'admin', 'system_user, system_admin', 'root_user','super_user',
                                             'administrator')
                    THEN TRUE ELSE FALSE END)                                                               AS system_admin
           , MAX(CASE WHEN COALESCE(e.user_actual_role, 'none') in ('system_admin', 'system_admin, system_user', 'system_administrator',
                                             'admin_user', 'admin', 'system_user, system_admin', 'root_user','super_user',
                                             'administrator')
                    THEN FALSE ELSE TRUE END)                                                                AS system_user
           , coalesce(count(e.id), 0)                                                  AS total_events
           , coalesce(count(CASE WHEN 
                                CASE WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('SEGMENT_WEBAPP_EVENTS','RUDDER_WEBAPP_EVENTS') 
                                        AND lower(COALESCE(e.context_user_agent, e.context_useragent)) LIKE '%electron%' THEN 'Desktop'
                                 WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('SEGMENT_WEBAPP_EVENTS','RUDDER_WEBAPP_EVENTS') 
                                        AND lower(COALESCE(e.context_user_agent, e.context_useragent)) NOT LIKE '%electron%' THEN 'WebApp'
                                 WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('SEGMENT_MOBILE_EVENTS','MOBILE_EVENTS') THEN 'Mobile'
                                 WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('PORTAL_EVENTS') THEN 'Customer Portal'
                                 ELSE 'WebApp' END = 'Desktop' 
                                    THEN e.id ELSE NULL END), 0)                               AS desktop_events
           , coalesce(count(CASE WHEN 
                                CASE WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('SEGMENT_WEBAPP_EVENTS','RUDDER_WEBAPP_EVENTS') 
                                        AND lower(COALESCE(e.context_user_agent, e.context_useragent)) LIKE '%electron%' THEN 'Desktop'
                                 WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('SEGMENT_WEBAPP_EVENTS','RUDDER_WEBAPP_EVENTS') 
                                        AND lower(COALESCE(e.context_user_agent, e.context_useragent)) NOT LIKE '%electron%' THEN 'WebApp'
                                 WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('SEGMENT_MOBILE_EVENTS','MOBILE_EVENTS') THEN 'Mobile'
                                 WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('PORTAL_EVENTS') THEN 'Customer Portal'
                                 ELSE 'WebApp' END = 'WebApp' 
                                    THEN e.id ELSE NULL END), 0)                               AS web_app_events
           , coalesce(count(CASE WHEN 
                                CASE WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('SEGMENT_WEBAPP_EVENTS','RUDDER_WEBAPP_EVENTS') 
                                        AND lower(COALESCE(e.context_user_agent, e.context_useragent)) LIKE '%electron%' THEN 'Desktop'
                                 WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('SEGMENT_WEBAPP_EVENTS','RUDDER_WEBAPP_EVENTS') 
                                        AND lower(COALESCE(e.context_user_agent, e.context_useragent)) NOT LIKE '%electron%' THEN 'WebApp'
                                 WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('SEGMENT_MOBILE_EVENTS','MOBILE_EVENTS') THEN 'Mobile'
                                 WHEN SPLIT_PART(e._dbt_source_relation2, '.', 3) IN ('PORTAL_EVENTS') THEN 'Customer Portal'
                                 ELSE 'WebApp' END = 'Mobile' 
                                    THEN e.id ELSE NULL END), 0)                               AS mobile_events
           , count(CASE WHEN e.category = 'action' THEN e.id ELSE NULL END)               AS action_events
           , count(CASE WHEN e.category = 'api' THEN e.id ELSE NULL END)            AS api_events
           , count(CASE WHEN e.category = 'gfycat' THEN e.id ELSE NULL END)         AS gfycat_events
           , count(CASE WHEN e.category = 'performance' THEN e.id ELSE NULL END)    AS performance_events
           , count(CASE WHEN e.category = 'plugins' THEN e.id ELSE NULL END)        AS plugins_events
           , count(CASE WHEN e.category = 'settings' THEN e.id ELSE NULL END)       AS settings_events
           , count(CASE WHEN e.category = 'signup' THEN e.id ELSE NULL END)         AS signup_events
           , count(CASE WHEN e.category = 'system_console' THEN e.id ELSE NULL END) AS system_console_events
           , count(CASE WHEN e.category = 'tutorial' THEN e.id ELSE NULL END)       AS tutorial_events
           , count(CASE WHEN e.category = 'ui' THEN e.id ELSE NULL END)             AS ui_events
           , MAX(e.received_at)                                                               AS max_timestamp
         FROM dates                                d
              LEFT JOIN {{ ref('user_events_telemetry') }} e
                        ON d.user_id = e.user_actual_id
                            AND COALESCE(d.server_id,'') = COALESCE(e.user_id, IFF(LENGTH(e.context_server) < 26, NULL, e.context_server),
                          IFF(LENGTH(e.context_traits_userid) < 26, NULL,e.context_traits_userid),
                          IFF(LENGTH(e.context_server) < 26, NULL, e.context_server))
                            AND d.date = e.received_at::date
         GROUP BY 1, 2, 3),

     mau                     AS (
         SELECT
             e1.user_id
           , e1.date
           , e1.server_id
           , MIN(m.min_active_date) OVER (PARTITION BY e1.user_id)                                              AS min_active_date
           , CASE WHEN MIN(m.min_active_date) OVER (PARTITION BY e1.user_id) = e1.date AND e1.total_events > 0 THEN 'First Time MAU'
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
                   AND COALESCE(e1.server_id, '') = COALESCE(m.server_id, '')
         WHERE e1.date < CURRENT_DATE
     ),
     user_events_by_date_agg AS (
         SELECT
             e1.date
           , e1.user_id
           , e1.server_id                                                  AS server_id
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
           , MIN(m.min_active_date)                                    AS first_active_date
           , MAX(m.last_active_date)                                   AS last_active_date
           , MAX(m.events_last_30_days)                                AS events_last_30_days
           , MAX(m.events_last_31_days)                                AS events_last_31_days
           , MAX(m.events_alltime)                                     AS events_alltime
           , MAX(m.max_events)                                         AS max_events
           , MAX(m.mobile_events_last_30_days)                         AS mobile_events_last_30_days
           , MAX(m.mobile_events_alltime)                              AS mobile_events_alltime
           , MAX(m.max_mobile_events)                                  AS max_mobile_events
           , MAX(e1.max_timestamp)                                     AS max_timestamp
           , {{ dbt_utils.surrogate_key(['e1.date', 'e1.user_id', 'e1.server_id']) }}    AS id
         FROM events   e1
              JOIN mau m
                   ON e1.user_id = m.user_id
                       AND e1.date = m.date
                       AND coalesce(e1.server_id, '') = coalesce(m.server_id, '')
         WHERE e1.date < CURRENT_DATE
         {% if is_incremental() %}

         AND e1.max_timestamp > (SELECT MAX(max_timestamp) FROM {{this}})

         {% endif %}
         GROUP BY 1, 2, 3, 33)
SELECT *
FROM user_events_by_date_agg