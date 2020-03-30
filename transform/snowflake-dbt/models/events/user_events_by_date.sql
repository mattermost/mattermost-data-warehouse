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
      , CASE
          WHEN context_user_agent LIKE '%Electron/%' THEN 'Electron'
          WHEN context_user_agent LIKE '%Edge/%'     THEN 'Edge'
          WHEN context_user_agent LIKE '%Edg/%'      THEN 'Edge'
          WHEN context_user_agent LIKE '%MSIE%'      THEN 'IE'
          WHEN context_user_agent LIKE '%Trident/%'  THEN 'IE'
          WHEN context_user_agent LIKE '%Chrome/%'   THEN 'Chrome'
          WHEN context_user_agent LIKE '%Firefox/%'  THEN 'Firefox'
          WHEN context_user_agent LIKE '%Safari/%'   THEN 'Safari'
          ELSE 'Other'
          END                   AS browser
      , CASE
          WHEN context_user_agent LIKE '%iPhone%'    THEN 'iPhone'
          WHEN context_user_agent LIKE '%iPad%'      THEN 'iPad'
          WHEN context_user_agent LIKE '%CrOS%'      THEN 'Chrome OS'
          WHEN context_user_agent LIKE '%Android%'   THEN 'Android'
          WHEN context_user_agent LIKE '%Macintosh%' THEN 'Mac'
          WHEN context_user_agent LIKE '%Windows%'   THEN 'Windows'
          WHEN context_user_agent LIKE '%Linux%'     THEN 'Linux'
          ELSE 'Other'
          END                   AS os
      , CASE
          WHEN context_user_agent LIKE '%Electron/%'
                                                    THEN split_part(split_part(context_user_agent, 'Mattermost/', 2), ' ', 1)
          WHEN context_user_agent LIKE '%Edge/%'    THEN split_part(split_part(context_user_agent, 'Edge/', 2), ' ', 1)
          WHEN context_user_agent LIKE '%Edg/%'     THEN split_part(split_part(context_user_agent, 'Edg/', 2), ' ', 1)
          WHEN context_user_agent LIKE '%Trident/%' THEN split_part(split_part(context_user_agent, 'Trident/', 2), ' ', 1)
          WHEN context_user_agent LIKE '%Chrome/%'  THEN split_part(split_part(context_user_agent, 'Chrome/', 2), ' ', 1)
          WHEN context_user_agent LIKE '%Firefox/%' THEN split_part(split_part(context_user_agent, 'Firefox/', 2), ' ', 1)
          WHEN context_user_agent LIKE '%Safari/%'  THEN split_part(split_part(context_user_agent, 'Version/', 2), ' ', 1)
          ELSE 'Other'
          END                   AS version
      , LOWER(m.type)           AS event_name
      , 'mobile'                AS event_type
      , COUNT(*)                AS num_events
    FROM {{ source('mattermost_rn_mobile_release_builds_v2', 'event')}} m
    WHERE user_actual_id IS NOT NULL
    AND m.timestamp::DATE <= CURRENT_DATE - INTERVAL '1 DAY'
    {% if is_incremental() %}

      AND timestamp::DATE > (SELECT MAX(date) from {{this}})

    {% endif %}
    GROUP BY 1, 2, 3, 5, 6, 7, 8, 9
),
     events              AS (
         SELECT
             e.timestamp::DATE                                                                         AS date
           , e.user_id                                                                                 AS server_id
           , e.user_actual_id                                                                          AS user_id
           , min(e.user_actual_role)                                                                   AS user_role
           , CASE
              WHEN context_user_agent LIKE '%Electron/%' THEN 'Electron'
              WHEN context_user_agent LIKE '%Edge/%'     THEN 'Edge'
              WHEN context_user_agent LIKE '%Edg/%'      THEN 'Edge'
              WHEN context_user_agent LIKE '%MSIE%'      THEN 'IE'
              WHEN context_user_agent LIKE '%Trident/%'  THEN 'IE'
              WHEN context_user_agent LIKE '%Chrome/%'   THEN 'Chrome'
              WHEN context_user_agent LIKE '%Firefox/%'  THEN 'Firefox'
              WHEN context_user_agent LIKE '%Safari/%'   THEN 'Safari'
              ELSE 'Other'
              END                                                                                        AS browser
           , CASE
              WHEN context_user_agent LIKE '%iPhone%'    THEN 'iPhone'
              WHEN context_user_agent LIKE '%iPad%'      THEN 'iPad'
              WHEN context_user_agent LIKE '%CrOS%'      THEN 'Chrome OS'
              WHEN context_user_agent LIKE '%Android%'   THEN 'Android'
              WHEN context_user_agent LIKE '%Macintosh%' THEN 'Mac'
              WHEN context_user_agent LIKE '%Windows%'   THEN 'Windows'
              WHEN context_user_agent LIKE '%Linux%'     THEN 'Linux'
              ELSE 'Other'
              END                                                                                      AS os
           , CASE
              WHEN context_user_agent LIKE '%Electron/%'
                                                        THEN split_part(split_part(context_user_agent, 'Mattermost/', 2), ' ', 1)
              WHEN context_user_agent LIKE '%Edge/%'    THEN split_part(split_part(context_user_agent, 'Edge/', 2), ' ', 1)
              WHEN context_user_agent LIKE '%Edg/%'     THEN split_part(split_part(context_user_agent, 'Edg/', 2), ' ', 1)
              WHEN context_user_agent LIKE '%Trident/%' THEN split_part(split_part(context_user_agent, 'Trident/', 2), ' ', 1)
              WHEN context_user_agent LIKE '%Chrome/%'  THEN split_part(split_part(context_user_agent, 'Chrome/', 2), ' ', 1)
              WHEN context_user_agent LIKE '%Firefox/%' THEN split_part(split_part(context_user_agent, 'Firefox/', 2), ' ', 1)
              WHEN context_user_agent LIKE '%Safari/%'  THEN split_part(split_part(context_user_agent, 'Version/', 2), ' ', 1)
              ELSE 'Other'
              END                                                                                      AS version
           , LOWER(e.type)                                                                             AS event_name
           , CASE WHEN LOWER(e.context_user_agent) LIKE '%electron%' THEN 'desktop' ELSE 'web_app' END AS event_type
           , COUNT(*)                                                                                  AS num_events
         FROM {{ source('mattermost2', 'event') }} e
         WHERE user_actual_id IS NOT NULL
         AND e.timestamp::DATE <= CURRENT_DATE - INTERVAL '1 DAY'
         {% if is_incremental() %}

          AND timestamp::DATE > (SELECT MAX(date) from {{this}})

         {% endif %}
         GROUP BY 1, 2, 3, 5, 6, 7, 8, 9
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
           , e.os
           , e.browser
           , r.version                                                                                AS browser_version
           , r.event_id                                                                               AS event_id
           , e.event_name
           , sum(e.num_events)                                                                        AS total_events
           , sum(CASE WHEN e.event_type = 'desktop' THEN e.num_events ELSE 0 END)                     AS desktop_events
           , sum(CASE WHEN e.event_type = 'web_app' THEN e.num_events ELSE 0 END)                     AS web_app_events
           , sum(CASE WHEN e.event_type = 'mobile' THEN e.num_events ELSE 0 END)                      AS mobile_events
         FROM all_events                  e
              LEFT JOIN {{ ref('events_registry') }} r
                   ON e.event_name = r.event_name
         GROUP BY 1, 2, 3, 7, 8, 9, 10, 11
     )
SELECT *
FROM user_events_by_date
