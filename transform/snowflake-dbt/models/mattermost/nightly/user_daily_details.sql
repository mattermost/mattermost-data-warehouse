{{config({
    "materialized": 'incremental',
    "schema": "mattermost",
    "enabled":false
  })
}}

WITH user_events       AS (
    SELECT
        e1.date
      , TRIM(e1.user_id)                                                                                                     AS user_id
      , MAX(CASE WHEN e1.server_id IS NOT NULL THEN TRIM(e1.server_id) ELSE NULL END)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)   AS server_id
      , MIN(CASE WHEN e1.total_events > 0 THEN e1.date ELSE NULL END)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS first_mau_date
      , MAX(CASE WHEN e1.total_events > 0 THEN e1.date ELSE NULL END)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS last_mau_date
      , DATEDIFF(DAY, MIN(CASE WHEN e1.total_events > 0 THEN e1.date ELSE NULL END)
                          OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                 MAX(CASE WHEN e1.total_events > 0 THEN e1.date ELSE NULL END)
                     OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS days_first_to_last_mau
      , MIN(CASE WHEN e1.web_app_events > 0 THEN e1.date ELSE NULL END)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS first_webapp_date
      , MAX(CASE WHEN e1.web_app_events > 0 THEN e1.date ELSE NULL END)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS last_webapp_date
      , COUNT(CASE WHEN e1.web_app_events > 0 THEN e1.date ELSE NULL END)
              OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS webapp_active_days
      , MIN(CASE WHEN e1.desktop_events > 0 THEN e1.date ELSE NULL END)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS first_desktop_date
      , MAX(CASE WHEN e1.desktop_events > 0 THEN e1.date ELSE NULL END)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS last_desktop_date
      , COUNT(CASE WHEN e1.desktop_events > 0 THEN e1.date ELSE NULL END)
              OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS desktop_active_days
      , MIN(CASE WHEN e1.mobile_events > 0 THEN e1.date ELSE NULL END)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS first_mobile_date
      , MAX(CASE WHEN e1.mobile_events > 0 THEN e1.date ELSE NULL END)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS last_mobile_date
      , COUNT(CASE WHEN e1.mobile_events > 0 THEN e1.date ELSE NULL END)
              OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS mobile_active_days
      , COUNT(CASE WHEN e1.mau THEN e1.date ELSE NULL END)
              OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS days_in_mau
      , COUNT(CASE WHEN e1.mau_segment = 'Reengaged MAU' THEN e1.date ELSE NULL END)
              OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS reengaged_count
      , COUNT(CASE
                  WHEN NOT e1.mau AND e1.events_alltime > 0
                      THEN e1.date
                      ELSE NULL END)
              OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS days_not_in_mau
      , COUNT(CASE WHEN e1.mau_segment = 'Newly Disengaged' THEN e1.date ELSE NULL END)
              OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS disengaged_count
      , COUNT(CASE WHEN e1.active THEN e1.date ELSE NULL END)
              OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS days_active
      , COUNT(CASE
                  WHEN NOT e1.active AND e1.events_alltime > 0
                      THEN e1.date
                      ELSE NULL END)
              OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS days_inactive
      , MAX(e1.events_alltime)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS events_alltime
      , ROUND(SUM(e1.total_events)
                  OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
              NULLIF(COUNT(CASE WHEN e1.events_alltime > 0 THEN e1.date ELSE NULL END)
                    OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0),
              2)                                                                                                             AS avg_events_per_day
      , SUM(e1.web_app_events)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS webapp_events_alltime
      , ROUND(SUM(e1.web_app_events)
                  OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
              NULLIF(COUNT(CASE WHEN e1.events_alltime > 0 THEN e1.date ELSE NULL END)
                    OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0),
              2)                                                                                                             AS avg_webapp_events_per_day
      , SUM(e1.desktop_events)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS desktop_events_alltime
      , ROUND(SUM(e1.desktop_events)
                  OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
              NULLIF(COUNT(CASE WHEN e1.events_alltime > 0 THEN e1.date ELSE NULL END)
                    OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0),
              2)                                                                                                             AS avg_desktop_events_per_day
      , MAX(e1.mobile_events_alltime)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS mobile_events_alltime
      , ROUND(SUM(e1.mobile_events)
                  OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
              NULLIF(COUNT(CASE WHEN e1.events_alltime > 0 THEN e1.date ELSE NULL END)
                    OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0),
              2)                                                                                                             AS avg_mobile_events_per_day
      , MIN(s.first_active_date)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)   AS server_install_date
      , MAX(s.account_sfid)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)   AS account_sfid
      , MAX(s.last_license_id1)
            OVER (PARTITION BY TRIM(e1.user_id) ORDER BY e1.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)   AS license_id
    FROM {{ ref('user_events_by_date_agg') }}   e1
         LEFT JOIN {{ ref('server_fact') }} s
                   ON TRIM(e1.server_id) = TRIM(s.server_id)
    WHERE LENGTH(e1.user_id) < 36
                          ),
     min_nps_date      AS (
         SELECT
             user_id
           , min(last_score_date) AS min_score_date
         FROM {{ ref('nps_user_daily_score') }}
         GROUP BY 1
     ),
     dates             AS (
         SELECT
             d.date
           , m.user_id
           , m.min_score_date
         FROM {{ source('util', 'dates') }}        d
              JOIN min_nps_date m
                   ON d.date >= m.min_score_date
                       AND d.date <= CURRENT_DATE - INTERVAL '1 DAY'
     ),
     user_nps          AS (
         SELECT
             d.date
           , nps.user_id
           , max(CASE WHEN nps.server_id IS NOT NULL THEN nps.server_id ELSE NULL END)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS server_id
           , min(CASE WHEN nps.server_id IS NOT NULL THEN nps.server_install_date ELSE NULL END)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS server_install_date
           , min(nps.user_created_at)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_created_at
           , min(nps.last_score_date)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS first_nps_date
           , min(CASE WHEN d.min_score_date = nps.last_score_date THEN nps.score ELSE NULL END)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS first_nps_score
           , max(nps.last_score_date)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS last_nps_date
           , nps.score                                                                                                    AS last_nps_score
           , avg(nps.score)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS avg_nps_score
           , max(nps.responses_alltime)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS nps_responses_alltime
           , min(nps.last_feedback_date)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS first_nps_feedback_date
           , max(nps.last_feedback_date)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS last_nps_feedback_date
           , nps.feedback                                                                                                 AS all_nps_feedback
           , max(nps.feedback_count_alltime)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS nps_feedback_responses_alltime
           , max(nps.license_id)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS license_id
           , max(s.server_id)
                 OVER (PARTITION BY nps.user_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS account_sfid
         FROM dates                                  d
              JOIN {{ ref('nps_user_daily_score') }} nps
                   ON d.user_id = nps.user_id
                       AND date_trunc('day', d.date) = nps.date
              LEFT JOIN {{ ref('server_fact') }}       s
                        ON TRIM(nps.server_id) = TRIM(s.server_id)
     ),
     user_fact_grouped AS (
         SELECT
             COALESCE(e.date, n.date)                               AS date
           , COALESCE(e.user_id, n.user_id)                         AS user_id
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
         GROUP BY 1, 2, 3, 4, 5, 6, 7
     ),
     user_daily_details         AS (
         SELECT
             date
           , u.user_id                                                  AS user_id
           , u.user_created_at                                          AS user_created_at
           , u.server_id                                                AS server_id
           , MIN(u.server_install_date) OVER (PARTITION BY u.server_id) AS server_install_date
           , u.account_sfid                                             AS account_sfid
           , u.license_id                                               AS license_id
           , u.first_active_date                                        AS first_active_date
           , u.last_active_date                                         AS last_active_date
           , u.days_first_to_last_active                                AS days_first_to_last_active
           , u.first_webapp_date                                        AS first_webapp_date
           , u.last_webapp_date                                         AS last_webapp_date
           , u.webapp_active_days                                       AS webapp_active_days
           , u.first_desktop_date                                       AS first_desktop_date
           , u.last_desktop_date                                        AS last_desktop_date
           , u.desktop_active_days                                      AS desktop_active_days
           , u.first_mobile_date                                        AS first_mobile_date
           , u.last_mobile_date                                         AS last_mobile_date
           , u.mobile_active_days                                       AS mobile_active_days
           , u.days_in_mau                                              AS days_in_mau
           , u.reengaged_count                                          AS reengaged_count
           , u.days_not_in_mau                                          AS days_not_in_mau
           , u.disengaged_count                                         AS disengaged_count
           , u.days_active                                              AS days_active
           , u.days_inactive                                            AS days_inactive
           , u.events_alltime                                           AS events_alltime
           , u.avg_events_per_day                                       AS avg_events_per_day
           , u.webapp_events_alltime                                    AS webapp_events_alltime
           , u.avg_webapp_events_per_day                                AS avg_webapp_events_per_day
           , u.desktop_events_alltime                                   AS desktop_events_alltime
           , u.avg_desktop_events_per_day                               AS avg_desktop_events_per_day
           , u.mobile_events_alltime                                    AS mobile_events_alltime
           , u.avg_mobile_events_per_day                                AS avg_mobile_events_per_day
           , u.first_nps_date                                           AS first_nps_date
           , u.first_nps_score                                          AS first_nps_score
           , u.last_nps_date                                            AS last_nps_date
           , u.last_nps_score                                           AS last_nps_score
           , u.avg_nps_score                                            AS avg_nps_score
           , u.nps_responses_alltime                                    AS nps_responses_alltime
           , u.first_nps_feedback_date                                  AS first_nps_feedback_date
           , u.last_nps_feedback_date                                   AS last_nps_feedback_date
           , u.all_nps_feedback                                         AS all_nps_feedback
         FROM user_fact_grouped u
        {% if is_incremental() %}

        WHERE u.date > (SELECT MAX(date) FROM {{this}} )
        
        {% endif %}
                  )
SELECT *
FROM user_daily_details