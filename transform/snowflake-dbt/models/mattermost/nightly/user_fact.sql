{{config({
    "materialized": 'table',
    "schema": "mattermost",
    "enabled":false
  })
}}

WITH max_date  AS (
    SELECT
        user_id
      , MAX(date) AS max_date
      , MIN(date) AS min_date
      , COUNT(DISTINCT server_id) AS server_count
    FROM {{ ref('user_daily_details') }}
    GROUP BY 1
                  ),
    user_events AS (
      SELECT
        user_id
      , MAX(CASE WHEN chronological_sequence = 1 THEN EVENT_NAME else null end) AS first_event
      , MAX(CASE WHEN chronological_sequence = 2 THEN EVENT_NAME else null end) AS second_event
      , MAX(CASE WHEN chronological_sequence = 3 THEN EVENT_NAME else null end) AS third_event
      , MAX(CASE WHEN chronological_sequence = 4 THEN EVENT_NAME else null end) AS fourth_event
      , MAX(CASE WHEN chronological_sequence = 5 THEN EVENT_NAME else null end) AS fifth_event
      , MAX(CASE WHEN chronological_sequence = 6 THEN EVENT_NAME else null end) AS sixth_event
      , MAX(CASE WHEN chronological_sequence = 7 THEN EVENT_NAME else null end) AS seventh_event
      , MAX(CASE WHEN chronological_sequence = 8 THEN EVENT_NAME else null end) AS eighth_event
      , MAX(CASE WHEN chronological_sequence = 9 THEN EVENT_NAME else null end) AS ninth_event
      , MAX(CASE WHEN chronological_sequence = 10 THEN EVENT_NAME else null end) AS tenth_event
      , COUNT(DISTINCT ID)                                                       AS events_performed
      , MAX(user_role)                                                          AS user_role
      FROM {{ ref('user_events_by_date') }}
      WHERE chronological_sequence BETWEEN 1 AND 10
      GROUP BY 1
    ),
    first_server AS (
      SELECT
          u.user_id
        , u.server_id as first_server_id
      FROM {{ ref('user_daily_details') }} u
      JOIN max_date m
           ON u.user_id = m.user_id
           AND u.date = m.min_date
      GROUP BY 1, 2
    ),
     user_fact AS (
         SELECT
             u.user_id
           , e.user_role
           , u.user_created_at
           , u.server_id
           , fs.first_server_id
           , m.server_count
           , u.server_install_date
           , u.account_sfid
           , u.license_id
           , e.first_event
           , e.second_event
           , e.third_event
           , e.fourth_event
           , e.fifth_event
           , e.sixth_event
           , e.seventh_event
           , e.eighth_event
           , e.ninth_event
           , e.tenth_event
           , e.events_performed
           , u.first_active_date
           , u.last_active_date
           , u.days_first_to_last_active
           , u.first_webapp_date
           , u.last_webapp_date
           , u.webapp_active_days
           , u.first_desktop_date
           , u.last_desktop_date
           , u.desktop_active_days
           , u.first_mobile_date
           , u.last_mobile_date
           , u.mobile_active_days
           , u.days_in_mau
           , u.reengaged_count
           , u.days_not_in_mau
           , u.disengaged_count
           , u.days_active
           , u.days_inactive
           , u.events_alltime
           , u.avg_events_per_day
           , u.webapp_events_alltime
           , u.avg_webapp_events_per_day
           , u.desktop_events_alltime
           , u.avg_desktop_events_per_day
           , u.mobile_events_alltime
           , u.avg_mobile_events_per_day
           , u.first_nps_date
           , u.first_nps_score
           , u.last_nps_date
           , u.last_nps_score
           , u.avg_nps_score
           , u.nps_responses_alltime
           , u.first_nps_feedback_date
           , u.last_nps_feedback_date
           , u.all_nps_feedback
         FROM {{ ref('user_daily_details') }} u
              JOIN max_date                 m
                   ON u.user_id = m.user_id
                       AND u.date = m.max_date
              JOIN user_events e
                   ON u.user_id = e.user_id
              JOIN first_server fs
                   ON u.user_id = fs.user_id
         {{ dbt_utils.group_by(55)}}
     )
SELECT *
FROM user_fact