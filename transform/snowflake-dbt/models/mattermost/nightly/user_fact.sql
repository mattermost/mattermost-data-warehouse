{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH user_events AS (
    SELECT
        TRIM(e1.user_id)                                                                AS user_id
      , MAX(CASE WHEN e2.user_id IS NOT NULL THEN TRIM(e1.server_id) ELSE NULL END)     AS server_id
      , MIN(CASE WHEN e1.total_events > 0 THEN e1.date ELSE NULL END)                   AS first_mau_date
      , MAX(CASE WHEN e1.total_events > 0 THEN e1.date ELSE NULL END)                   AS last_mau_date
      , DATEDIFF(DAY, MIN(CASE WHEN e1.total_events > 0 THEN e1.date ELSE NULL END),
                 MAX(CASE WHEN e1.total_events > 0 THEN e1.date ELSE NULL END))         AS days_first_to_last_mau
      , MIN(CASE WHEN e1.web_app_events > 0 THEN e1.date ELSE NULL END)                 AS first_webapp_date
      , MAX(CASE WHEN e1.web_app_events > 0 THEN e1.date ELSE NULL END)                 AS last_webapp_date
      , COUNT(CASE WHEN e1.web_app_events > 0 THEN e1.date ELSE NULL END)               AS webapp_active_days
      , MIN(CASE WHEN e1.desktop_events > 0 THEN e1.date ELSE NULL END)                 AS first_desktop_date
      , MAX(CASE WHEN e1.desktop_events > 0 THEN e1.date ELSE NULL END)                 AS last_desktop_date
      , COUNT(CASE WHEN e1.desktop_events > 0 THEN e1.date ELSE NULL END)               AS desktop_active_days
      , MIN(CASE WHEN e1.mobile_events > 0 THEN e1.date ELSE NULL END)                  AS first_mobile_date
      , MAX(CASE WHEN e1.mobile_events > 0 THEN e1.date ELSE NULL END)                  AS last_mobile_date
      , COUNT(CASE WHEN e1.mobile_events > 0 THEN e1.date ELSE NULL END)                AS mobile_active_days
      , COUNT(CASE WHEN e1.mau THEN e1.date ELSE NULL END)                              AS days_in_mau
      , COUNT(CASE WHEN e1.mau_segment = 'Reengaged MAU' THEN e1.date ELSE NULL END)    AS reengaged_count
      , COUNT(CASE WHEN NOT e1.mau THEN e1.date ELSE NULL END)                          AS days_not_in_mau
      , COUNT(CASE WHEN e1.mau_segment = 'Newly Disengaged' THEN e1.date ELSE NULL END) AS disengaged_count
      , COUNT(CASE WHEN e1.active THEN e1.date ELSE NULL END)                           AS days_active
      , COUNT(CASE WHEN NOT e1.active THEN e1.date ELSE NULL END)                       AS days_inactive
      , MAX(e1.events_alltime)                                                          AS events_alltime
      , ROUND(SUM(e1.total_events) / COUNT(e1.date), 2)                                 AS avg_events_per_day
      , SUM(e1.web_app_events)                                                          AS webapp_events_alltime
      , ROUND(SUM(e1.web_app_events) / COUNT(e1.date), 2)                               AS avg_webapp_events_per_day
      , SUM(e1.desktop_events)                                                          AS desktop_events_alltime
      , ROUND(SUM(e1.desktop_events) / COUNT(e1.date), 2)                               AS avg_desktop_events_per_day
      , MAX(e1.mobile_events_alltime)                                                   AS mobile_events_alltime
      , ROUND(SUM(e1.mobile_events) / COUNT(e1.date), 2)                                AS avg_mobile_events_per_day
      , MIN(s.first_active_date)                                                        AS server_install_date
      , MAX(s.last_account_sfid)                                                        AS account_sfid
      , MAX(s.last_license_id1)                                                         AS license_id
    FROM {{ ref('user_events_by_date_agg') }}   e1
         LEFT JOIN (
        SELECT
            TRIM(user_id) AS user_id
          , MAX(date)     AS server_date
        FROM {{ ref('user_events_by_date_agg') }}
        WHERE server_id IS NOT NULL
        GROUP BY 1)                       e2
                   ON e1.user_id = e2.user_id
                       AND e1.date = e2.server_date
         LEFT JOIN {{ ref('server_fact') }} s
                   ON TRIM(e1.server_id) = TRIM(s.server_id)
    GROUP BY 1
),
     user_nps    AS (
         SELECT
             TRIM(n1.user_id)                                                                    AS user_id
           , MAX(CASE WHEN n1.month = n2.server_month THEN TRIM(n1.server_id) ELSE NULL END)     AS server_id
           , MIN(n1.server_install_date)                                                         AS server_install_date
           , MIN(n1.user_created_at)                                                             AS user_created_at
           , MIN(n1.last_score_date)                                                             AS first_nps_date
           , MAX(CASE WHEN n1.last_score_date = n2.first_nps_date THEN n1.score ELSE NULL END)   AS first_nps_score
           , MAX(n1.last_score_date)                                                             AS last_nps_date
           , MAX(CASE WHEN n1.last_score_date = n2.last_nps_date THEN n1.score ELSE NULL END)    AS last_nps_score
           , AVG(DISTINCT n1.score)                                                              AS avg_nps_score
           , MAX(n1.responses_alltime)                                                           AS nps_responses_alltime
           , MIN(n1.last_feedback_date)                                                          AS first_nps_feedback_date
           , MAX(n1.last_feedback_date)                                                          AS last_nps_feedback_date
           , LISTAGG(DISTINCT n1.feedback, '; ') WITHIN GROUP ( ORDER BY n1.feedback ) AS all_nps_feedback
           , MAX(COALESCE(CASE WHEN n1.month = n2.license_month THEN n1.license_id ELSE NULL END,
                          s.last_license_id1))                                                   AS license_id
           , MAX(s.last_account_sfid)                                                            AS account_sfid
         FROM {{ ref('nps_user_monthly_score') }} n1
              LEFT JOIN (SELECT
                             TRIM(user_id)                                                  AS user_id
                           , MAX(CASE WHEN server_id IS NOT NULL THEN month ELSE NULL END)  AS server_month
                           , MAX(CASE WHEN license_id IS NOT NULL THEN month ELSE NULL END) AS license_month
                           , MIN(last_score_date)                                           AS first_nps_date
                           , MAX(last_score_date)                                           AS last_nps_date
                         FROM {{ ref('nps_user_monthly_score') }}
                         WHERE server_id IS NOT NULL
                         GROUP BY 1)            n2
                        ON n1.user_id = n2.user_id
              LEFT JOIN {{ ref('server_fact') }} s
                        ON TRIM(n1.server_id) = TRIM(s.server_id)
         GROUP BY 1
     ),
     user_fact   AS (
         SELECT
             COALESCE(e.user_id, n.user_id)                         AS user_id
           , COALESCE(n.user_created_at, e.first_mau_date)          AS user_created_at
           , COALESCE(e.server_id, n.server_id)                     AS server_id
           , COALESCE(e.server_install_date, n.server_install_date) AS server_install_date
           , COALESCE(e.account_sfid, n.account_sfid)               AS account_sfid
           , COALESCE(e.license_id, n.license_id)                   AS license_id
           , MAX(e.first_mau_date)                                  AS first_active_date
           , MAX(e.last_mau_date)                                   AS last_active_date
           , MAX(e.days_first_to_last_mau)                          AS days_first_to_last_active
           , MAX(e.first_webapp_date)                               AS first_webapp_date
           , MAX(e.last_webapp_date)                                AS last_webapp_date
           , MAX(e.webapp_active_days)                              AS webapp_active_days
           , MAX(e.first_desktop_date)                              AS first_desktop_date
           , MAX(e.last_desktop_date)                               AS last_desktop_date
           , MAX(e.desktop_active_days)                             AS desktop_active_days
           , MAX(e.first_mobile_date)                               AS first_mobile_date
           , MAX(e.last_mobile_date)                                AS last_mobile_date
           , MAX(e.mobile_active_days)                              AS mobile_active_days
           , MAX(e.days_in_mau)                                     AS days_in_mau
           , MAX(e.reengaged_count)                                 AS reengaged_count
           , MAX(e.days_not_in_mau)                                 AS days_not_in_mau
           , MAX(e.disengaged_count)                                AS disengaged_count
           , MAX(e.days_active)                                     AS days_active
           , MAX(e.days_inactive)                                   AS days_inactive
           , MAX(e.events_alltime)                                  AS events_alltime
           , MAX(e.avg_events_per_day)                              AS avg_events_per_day
           , MAX(e.webapp_events_alltime)                           AS webapp_events_alltime
           , MAX(e.avg_webapp_events_per_day)                       AS avg_webapp_events_per_day
           , MAX(e.desktop_events_alltime)                          AS desktop_events_alltime
           , MAX(e.avg_desktop_events_per_day)                      AS avg_desktop_events_per_day
           , MAX(e.mobile_events_alltime)                           AS mobile_events_alltime
           , MAX(e.avg_mobile_events_per_day)                       AS avg_mobile_events_per_day
           , MAX(n.first_nps_date)                                  AS first_nps_date
           , MAX(n.first_nps_score)                                 AS first_nps_score
           , MAX(n.last_nps_date)                                   AS last_nps_date
           , MAX(n.last_nps_score)                                  AS last_nps_score
           , MAX(n.avg_nps_score)                                   AS avg_nps_score
           , MAX(n.nps_responses_alltime)                           AS nps_responses_alltime
           , MAX(n.first_nps_feedback_date)                         AS first_nps_feedback_date
           , MAX(n.last_nps_feedback_date)                          AS last_nps_feedback_date
           , MAX(n.all_nps_feedback)                                AS all_nps_feedback
         FROM user_events              e
              FULL OUTER JOIN user_nps n
                              ON e.user_id = n.user_id
         GROUP BY 1, 2, 3, 4, 5, 6
     )
SELECT *
FROM user_fact