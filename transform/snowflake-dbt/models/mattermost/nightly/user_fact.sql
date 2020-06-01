{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH max_date  AS (
    SELECT
        user_id
      , MAX(date) AS max_date
    FROM {{ ref('user_daily_details') }}
    GROUP BY 1
                  ),
     user_fact AS (
         SELECT
             u.user_id
           , u.user_created_at
           , u.server_id
           , u.server_install_date
           , u.account_sfid
           , u.license_id
           , e.event_name AS first_event
           , e.event_type AS first_event_client
           , e.user_context_agent AS first_event_user_agent
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
              LEFT JOIN {{ ref('user_events_by_date') }} e
                   ON u.user_id = e.user_id
                   AND e.chronological_sequence = 1
     )
SELECT *
FROM user_fact