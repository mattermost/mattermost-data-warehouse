{{config({
    "materialized": 'incremental',
    "schema": "events",
    "unique_key":'id'
  })
}}

WITH mobile_events       AS (
    SELECT
        m.timestamp::DATE                                                                         AS date
      , TRIM(m.user_id)                                                                           AS server_id
      , TRIM(m.user_actual_id)                                                                    AS user_id
      , MIN(m.user_actual_role)                                                                   AS user_role
      , CASE
          WHEN m.context_device_type = 'ios' THEN 'iPhone'            
          WHEN m.context_device_type = 'android' THEN 'Android'
          ELSE 'Other'
          END                                                                                     AS browser
      , CASE
          WHEN m.context_device_type = 'ios'    THEN 'iPhone'
          WHEN m.context_device_type = 'android'     THEN 'Android'
          ELSE 'Other'
          END                                                                                     AS os
      , CASE
          WHEN m.context_device_type = 'ios' THEN context_app_version::VARCHAR
          WHEN m.context_device_type = 'android' THEN m.context_app_version::VARCHAR
          ELSE 'Other'
          END                                                                                     AS version
      , CASE
          WHEN m.context_device_os IS NOT NULL THEN m.context_device_os::varchar
          ELSE 'Unknown' END                                                                      AS os_version
      , LOWER(m.type)                                                                             AS event_name
      , 'mobile'                                                                                  AS event_type
      , COUNT(m.*)                                                                                AS num_events
      , ''                                                                                        AS context_user_agent
      , MAX(m.timestamp)                                                                          AS max_timestamp
      , {{ dbt_utils.surrogate_key('m.timestamp::date', 'm.user_actual_id', 'm.user_id', 'm.context_device_type', 'context_device_os', 'm.context_app_version', 'lower(m.type)') }}                       AS id
    FROM {{ source('mattermost_rn_mobile_release_builds_v2', 'event')}} m
    WHERE TRIM(user_actual_id) IS NOT NULL
    AND m.timestamp::DATE <= CURRENT_DATE 
    {% if is_incremental() %}

      AND timestamp::date >= (SELECT MAX(date - interval '1 day') from {{this}})

    {% endif %}
    GROUP BY 1, 2, 3, 5, 6, 7, 8, 9, 10, 12, 14
),
     events              AS (
         SELECT
             e.timestamp::DATE                                                                         AS date
           , TRIM(e.user_id)                                                                           AS server_id
           , TRIM(e.user_actual_id)                                                                    AS user_id
           , min(e.user_actual_role)                                                                   AS user_role
           , CASE
              WHEN e.context_user_agent LIKE '%iPhone%'    THEN 'iPhone'
              WHEN e.context_user_agent LIKE '%iPad%'      THEN 'iPad'
              WHEN e.context_user_agent LIKE '%Android%'   THEN 'Android'
              WHEN e.context_user_agent LIKE '%Electron/%' THEN 'Electron'
              WHEN e.context_user_agent LIKE '%Edge/%'     THEN 'Edge'
              WHEN e.context_user_agent LIKE '%Edg/%'      THEN 'Edge'
              WHEN e.context_user_agent LIKE '%MSIE%'      THEN 'IE'
              WHEN e.context_user_agent LIKE '%Trident/%'  THEN 'IE'
              WHEN e.context_user_agent LIKE '%Firefox/%'  THEN 'Firefox'
              WHEN e.context_user_agent LIKE '%Chrome/%'   THEN 'Chrome'
              WHEN e.context_user_agent LIKE '%Safari/%'   THEN 'Safari'
              ELSE 'Other'
              END                                                                                        AS browser
           , CASE
              WHEN e.context_user_agent LIKE '%iPhone%'    THEN 'iPhone'
              WHEN e.context_user_agent LIKE '%iPad%'      THEN 'iPad'
              WHEN e.context_user_agent LIKE '%CrOS%'      THEN 'Chrome OS'
              WHEN e.context_user_agent LIKE '%Android%'   THEN 'Android'
              WHEN e.context_user_agent LIKE '%Macintosh%' THEN 'Mac'
              WHEN e.context_user_agent LIKE '%Windows%'   THEN 'Windows'
              WHEN e.context_user_agent LIKE '%Linux%'     THEN 'Linux'
              ELSE 'Other'
              END                                                                                      AS os
           , CASE
              WHEN e.context_user_agent LIKE '%Electron/%'
                                                        THEN split_part(split_part(e.context_user_agent, 'Mattermost/', 2), ' ', 1)
              WHEN e.context_user_agent LIKE '%Edge/%'    THEN split_part(split_part(e.context_user_agent, 'Edge/', 2), ' ', 1)
              WHEN e.context_user_agent LIKE '%Edg/%'     THEN split_part(split_part(e.context_user_agent, 'Edg/', 2), ' ', 1)
              WHEN e.context_user_agent LIKE '%Trident/%' THEN split_part(split_part(e.context_user_agent, 'Trident/', 2), ' ', 1)
              WHEN e.context_user_agent LIKE '%Chrome/%'  THEN split_part(split_part(e.context_user_agent, 'Chrome/', 2), ' ', 1)
              WHEN e.context_user_agent LIKE '%Firefox/%' THEN split_part(split_part(e.context_user_agent, 'Firefox/', 2), ' ', 1)
              WHEN e.context_user_agent LIKE '%Safari/%'  THEN split_part(split_part(e.context_user_agent, 'Version/', 2), ' ', 1)
              ELSE 'Other'
              END                                                                                      AS version
           , CASE
              WHEN e.context_user_agent LIKE '%iPhone%'    THEN split_part(split_part(e.context_user_agent, 'iPhone OS ', 2), ' ', 1)
              WHEN e.context_user_agent LIKE '%iPad%'      THEN split_part(split_part(e.context_user_agent, 'CPU OS ', 2), ' ', 1)
              WHEN e.context_user_agent LIKE '%CrOS%'      THEN split_part(
                split_part(split_part(e.context_user_agent, 'CrOS ', 2), ')', 1), 'CS', 1)
              WHEN e.context_user_agent LIKE '%Android%'
                                                   THEN split_part(split_part(e.context_user_agent, 'Android ', 2), ';', 1)
              WHEN e.context_user_agent LIKE '%Macintosh%'
                                                   THEN split_part(split_part(e.context_user_agent, 'Mac OS X ', 2), ')', 1)
              WHEN e.context_user_agent LIKE '%Windows%'   THEN split_part(
                split_part(split_part(e.context_user_agent, 'Windows ', 2), ')', 1), ';', 1)
              WHEN e.context_user_agent LIKE '%Linux%'     THEN split_part(split_part(e.context_user_agent, 'Linux ', 2), ')', 1)
              ELSE 'Unknown'
              END                                                                                      AS os_version
           , LOWER(e.type)                                                                             AS event_name
           , CASE WHEN LOWER(e.context_user_agent) LIKE '%electron%' THEN 'desktop' ELSE 'web_app' END AS event_type
           , COUNT(e.*)                                                                                AS num_events
           , context_user_agent
           , MAX(e.timestamp)                                                                          AS max_timestamp
           , {{ dbt_utils.surrogate_key('e.timestamp::date', 'e.user_actual_id', 'e.user_id', 'e.context_user_agent', 'lower(e.type)') }}                       AS id
         FROM {{ source('mattermost2', 'event') }} e
              LEFT JOIN {{ source('mm_telemetry_prod', 'event') }} rudder
                        ON e.timestamp::date = rudder.timestamp::date
                        AND e.user_actual_id = rudder.user_actual_id
                        AND e.user_id = rudder.user_id
                        AND e.type = rudder.type
                        AND e.context_user_agent = rudder.context_useragent
         WHERE rudder.user_actual_id IS NULL
         AND TRIM(e.user_actual_id) IS NOT NULL
         AND e.timestamp::DATE <= CURRENT_DATE
         {% if is_incremental() %}

          AND e.timestamp::date >= (SELECT MAX(date - interval '1 day') from {{this}})

         {% endif %}
         GROUP BY 1, 2, 3, 5, 6, 7, 8, 9, 10, 12, 14
     ),

          events2              AS (
         SELECT
             e.timestamp::DATE                                                                         AS date
           , TRIM(e.user_id)                                                                           AS server_id
           , TRIM(e.user_actual_id)                                                                    AS user_id
           , min(e.user_actual_role)                                                                   AS user_role
           , CASE
              WHEN context_useragent LIKE '%iPhone%'    THEN 'iPhone'
              WHEN context_useragent LIKE '%iPad%'      THEN 'iPad'
              WHEN context_useragent LIKE '%Android%'   THEN 'Android'
              WHEN context_useragent LIKE '%Electron/%' THEN 'Electron'
              WHEN context_useragent LIKE '%Edge/%'     THEN 'Edge'
              WHEN context_useragent LIKE '%Edg/%'      THEN 'Edge'
              WHEN context_useragent LIKE '%MSIE%'      THEN 'IE'
              WHEN context_useragent LIKE '%Trident/%'  THEN 'IE'
              WHEN context_useragent LIKE '%Firefox/%'  THEN 'Firefox'
              WHEN context_useragent LIKE '%Chrome/%'   THEN 'Chrome'
              WHEN context_useragent LIKE '%Safari/%'   THEN 'Safari'
              ELSE 'Other'
              END                                                                                        AS browser
           , CASE
              WHEN context_useragent LIKE '%iPhone%'    THEN 'iPhone'
              WHEN context_useragent LIKE '%iPad%'      THEN 'iPad'
              WHEN context_useragent LIKE '%CrOS%'      THEN 'Chrome OS'
              WHEN context_useragent LIKE '%Android%'   THEN 'Android'
              WHEN context_useragent LIKE '%Macintosh%' THEN 'Mac'
              WHEN context_useragent LIKE '%Windows%'   THEN 'Windows'
              WHEN context_useragent LIKE '%Linux%'     THEN 'Linux'
              ELSE 'Other'
              END                                                                                      AS os
           , CASE
              WHEN context_useragent LIKE '%Electron/%'
                                                        THEN split_part(split_part(context_useragent, 'Mattermost/', 2), ' ', 1)
              WHEN context_useragent LIKE '%Edge/%'    THEN split_part(split_part(context_useragent, 'Edge/', 2), ' ', 1)
              WHEN context_useragent LIKE '%Edg/%'     THEN split_part(split_part(context_useragent, 'Edg/', 2), ' ', 1)
              WHEN context_useragent LIKE '%Trident/%' THEN split_part(split_part(context_useragent, 'Trident/', 2), ' ', 1)
              WHEN context_useragent LIKE '%Chrome/%'  THEN split_part(split_part(context_useragent, 'Chrome/', 2), ' ', 1)
              WHEN context_useragent LIKE '%Firefox/%' THEN split_part(split_part(context_useragent, 'Firefox/', 2), ' ', 1)
              WHEN context_useragent LIKE '%Safari/%'  THEN split_part(split_part(context_useragent, 'Version/', 2), ' ', 1)
              ELSE 'Other'
              END                                                                                      AS version
           , CASE
              WHEN context_useragent LIKE '%iPhone%'    THEN split_part(split_part(context_useragent, 'iPhone OS ', 2), ' ', 1)
              WHEN context_useragent LIKE '%iPad%'      THEN split_part(split_part(context_useragent, 'CPU OS ', 2), ' ', 1)
              WHEN context_useragent LIKE '%CrOS%'      THEN split_part(
                split_part(split_part(context_useragent, 'CrOS ', 2), ')', 1), 'CS', 1)
              WHEN context_useragent LIKE '%Android%'
                                                   THEN split_part(split_part(context_useragent, 'Android ', 2), ';', 1)
              WHEN context_useragent LIKE '%Macintosh%'
                                                   THEN split_part(split_part(context_useragent, 'Mac OS X ', 2), ')', 1)
              WHEN context_useragent LIKE '%Windows%'   THEN split_part(
                split_part(split_part(context_useragent, 'Windows ', 2), ')', 1), ';', 1)
              WHEN context_useragent LIKE '%Linux%'     THEN split_part(split_part(context_useragent, 'Linux ', 2), ')', 1)
              ELSE 'Unknown'
              END                                                                                      AS os_version
           , LOWER(e.type)                                                                             AS event_name
           , CASE WHEN LOWER(e.context_useragent) LIKE '%electron%' THEN 'desktop' ELSE 'web_app' END AS event_type
           , COUNT(*)                                                                                  AS num_events
           , context_useragent                                                                         AS context_user_agent
           , MAX(e.timestamp)                                                                          AS max_timestamp
           , {{ dbt_utils.surrogate_key('e.timestamp::date', 'e.user_actual_id', 'e.user_id', 'e.context_useragent', 'lower(e.type)') }}                       AS id
         FROM {{ source('mm_telemetry_prod', 'event') }} e
         WHERE TRIM(user_actual_id) IS NOT NULL
         AND e.timestamp::DATE <= CURRENT_DATE
         {% if is_incremental() %}

          AND timestamp::date >= (SELECT MAX(date - interval '1 day') from {{this}})

         {% endif %}
         GROUP BY 1, 2, 3, 5, 6, 7, 8, 9, 10, 12, 14
     ),

     all_events          AS (
         SELECT *
         FROM mobile_events
         UNION ALL
         SELECT *
         FROM events
         UNION ALL
         SELECT *
         FROM events2
     ),

     user_events_by_date AS (
         SELECT
             e.date
           , e.server_id
           , e.user_id
           , e.event_type
           , max(e.user_role)                                                                         AS user_role
           , CASE WHEN MAX(split_part(e.user_role, ',', 1)) = 'system_admin' THEN TRUE ELSE FALSE END AS system_admin
           , CASE WHEN MAX(e.user_role) = 'system_user' THEN TRUE ELSE FALSE END                      AS system_user
           , e.os
           , e.browser
           , e.version                                                                                AS browser_version
           , e.os_version
           , r.event_id                                                                               AS event_id
           , e.event_name
           , sum(e.num_events)                                                                        AS total_events
           , sum(CASE WHEN e.event_type = 'desktop' THEN e.num_events ELSE 0 END)                     AS desktop_events
           , sum(CASE WHEN e.event_type = 'web_app' THEN e.num_events ELSE 0 END)                     AS web_app_events
           , sum(CASE WHEN e.event_type = 'mobile' THEN e.num_events ELSE 0 END)                      AS mobile_events
           , context_user_agent
           , max(max_timestamp)                                                                       AS max_timestamp
           , {{ dbt_utils.surrogate_key('e.date', 'e.user_id', 'e.server_id', 'e.context_user_agent', 'e.event_name', 'e.os', 'e.version') }}                      AS id
         FROM all_events                  e
              LEFT JOIN {{ ref('events_registry') }} r
                   ON e.event_name = r.event_name
         {% if is_incremental() %}

          WHERE date >= (SELECT MAX(date - interval '1 day') from {{this}})

         {% endif %}
         GROUP BY 1, 2, 3, 4, 8, 9, 10, 11, 12, 13, 18, 20
     )
SELECT *
FROM user_events_by_date
