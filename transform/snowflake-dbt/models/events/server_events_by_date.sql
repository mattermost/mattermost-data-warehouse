{{config({
    "materialized": 'incremental',
    "schema": "events",
    "unique_key": 'id'
  })
}}

WITH post_events AS (
  SELECT
      date
    , server_id
    , SUM(total_events) AS posts
    , COUNT(DISTINCT user_id) AS post_users
  FROM {{ ref('user_events_by_date') }}
  WHERE event_name = 'api_posts_create'
  GROUP BY 1, 2
),

server_events_by_date AS (
    SELECT
        events.date
      , events.server_id
      , {{ dbt_utils.surrogate_key('events.date', 'events.server_id') }}                            AS id
      , MIN(first_active_date)                                                                      AS first_active_date
      , MAX(last_active_date)                                                                       AS last_active_date
      , COUNT(DISTINCT CASE WHEN active THEN user_id ELSE NULL END)                                 AS dau_total
      , COUNT(DISTINCT CASE WHEN active AND mobile_events > 0 THEN user_id ELSE NULL END)           AS mobile_dau
      , COUNT(DISTINCT CASE WHEN active AND system_admin THEN user_id ELSE NULL END)                AS system_admin_dau
      , COUNT(DISTINCT CASE WHEN mau THEN user_id ELSE NULL END)                                    AS mau_total
      , COUNT(DISTINCT CASE WHEN mau AND mau_segment = 'First Time MAU' THEN user_id ELSE NULL END) AS first_time_mau
      , COUNT(DISTINCT CASE WHEN mau AND mau_segment = 'Reengaged MAU' THEN user_id ELSE NULL END)  AS reengaged_mau
      , COUNT(DISTINCT CASE WHEN mau AND mau_segment = 'Current MAU' THEN user_id ELSE NULL END)    AS current_mau
      , COUNT(DISTINCT CASE WHEN mau AND system_admin THEN user_id ELSE NULL END)                   AS system_admin_mau
      , SUM(total_events)                                                                           AS total_events
      , SUM(desktop_events)                                                                         AS desktop_events
      , SUM(web_app_events)                                                                         AS web_app_events
      , SUM(mobile_events)                                                                          AS mobile_events
      , SUM(action_events)                                                                          AS action_events
      , SUM(api_events)                                                                             AS api_events
      , SUM(gfycat_events)                                                                          AS gfycat_events
      , SUM(performance_events)                                                                     AS performance_events
      , SUM(plugin_events)                                                                          AS plugins_events
      , SUM(settings_events)                                                                        AS settings_events
      , SUM(signup_events)                                                                          AS signup_events
      , SUM(system_console_events)                                                                  AS system_console_events
      , SUM(tutorial_events)                                                                        AS tutorial_events
      , SUM(ui_events)                                                                              AS ui_events
      , SUM(events_last_30_days)                                                                    AS events_last_30_days
      , SUM(events_last_31_days)                                                                    AS events_last_31_days
      , SUM(events_alltime)                                                                         AS events_alltime
      , SUM(mobile_events_last_30_days)                                                             AS mobile_events_last_30_days
      , SUM(mobile_events_alltime)                                                                  AS mobile_events_alltime
      , COUNT(DISTINCT user_id)                                                                     AS users
      , MAX(posts.posts)                                                                            AS post_events
    FROM {{ ref('user_events_by_date_agg') }} events
    LEFT JOIN post_events posts
              ON events.server_id = posts.server_id
              AND events.date = posts.date
    {% if is_incremental() %}

    WHERE events.date >= (SELECT MAX(events.date) FROM {{this}})

    {% endif %}
    {{ dbt_utils.group_by(n=3) }}
)
SELECT *
FROM server_events_by_date