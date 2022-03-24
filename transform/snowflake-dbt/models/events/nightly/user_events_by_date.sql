{{config({
    "materialized": 'incremental',
    "schema": "events",
    "unique_key":'id',
    "tag":"nightly"
  })
}}

WITH mobile_events AS (
    SELECT
    cast({{ dbt_utils.string_literal(source('mattermost_rn_mobile_release_builds_v2', 'event')) }} as {{ dbt_utils.type_string() }}) as _DBT_SOURCE_RELATION
      , m.timestamp::DATE                                                                         AS date
      , TRIM(m.user_id)                                                                           AS server_id
      , COALESCE(TRIM(m.user_actual_id), NULL)                                                    AS user_id
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
          WHEN m.context_device_type = 'ios' THEN m.context_app_version::VARCHAR
          WHEN m.context_device_type = 'android' THEN m.context_app_version::VARCHAR
          ELSE 'Other'
          END                                                                                     AS version
      , CASE
          WHEN m.context_device_os IS NOT NULL THEN m.context_device_os::varchar
          ELSE 'Unknown' END                                                                      AS os_version
      , LOWER(m.type)                                                                             AS event_name
      , 'mobile'                                                                                  AS event_type
      , COUNT(m.timestamp)                                                                                AS num_events
      , ''                                                                                        AS context_user_agent
      , MAX(m.timestamp)                                                                          AS max_timestamp
      , MIN(m.timestamp)                                                                          AS min_timestamp
      , MAX(date_trunc('hour', m.uuid_ts) + interval '1 hour')                                    AS max_uuid_ts
      , m.category
      , {{ dbt_utils.surrogate_key(['m.timestamp::date', 'm.user_actual_id', 'm.user_id', 'm.context_device_type', 'context_device_os', 'm.context_app_version', 'm.context_device_os', 'lower(m.type)', 'm.category']) }}                       AS id
    FROM {{ source('mattermost_rn_mobile_release_builds_v2', 'event')}} m
    WHERE m.timestamp::DATE < CURRENT_DATE
    {% if is_incremental() %}

      AND m.timestamp > (SELECT MAX(max_timestamp) from {{this}}
      where _dbt_source_relation = {{ dbt_utils.string_literal(source('mattermost_rn_mobile_release_builds_v2', 'event')) }})

    {% endif %}
    GROUP BY 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13, 17, 18
),

mobile_events2 AS (
    SELECT
        cast({{ dbt_utils.string_literal(ref('mobile_events')) }} as {{ dbt_utils.type_string() }}) as _DBT_SOURCE_RELATION
      , m.timestamp::DATE                                                                         AS date
      , TRIM(m.user_id)                                                                           AS server_id
      , COALESCE(TRIM(m.user_actual_id), NULL)                                                    AS user_id
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
          WHEN m.context_device_type = 'ios' THEN context_app_build::VARCHAR
          WHEN m.context_device_type = 'android' THEN m.context_app_version::VARCHAR
          ELSE 'Other'
          END                                                                                     AS version
      , CASE
          WHEN m.context_os_version IS NOT NULL THEN m.context_os_version::varchar
          ELSE 'Unknown' END                                                                      AS os_version
      , COALESCE(LOWER(m.type), LOWER(m.event))                                                   AS event_name
      , 'mobile'                                                                                  AS event_type
      , COUNT(m.timestamp)                                                                        AS num_events
      , m.context_useragent                                                                       AS context_user_agent
      , MAX(m.timestamp)                                                                          AS max_timestamp
      , MIN(m.timestamp)                                                                          AS min_timestamp
      , MAX(NULL::TIMESTAMP)                                    AS max_uuid_ts
      , m.category
      , {{ dbt_utils.surrogate_key(['m.timestamp::date', 'm.user_actual_id', 'm.user_id', 'm.context_useragent', 'lower(m.type)', 'm.category']) }}                       AS id
    FROM {{ ref('mobile_events') }} m
    WHERE m.timestamp::DATE < CURRENT_DATE
    {% if is_incremental() %}

      AND m.timestamp > (SELECT MAX(max_timestamp) from {{this}}
            where _dbt_source_relation = {{ dbt_utils.string_literal(ref('mobile_events')) }})
    
    {% endif %}
    GROUP BY 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13, 17, 18
),
     events AS (
         SELECT
           cast({{ dbt_utils.string_literal(source('mattermost2', 'event')) }} as {{ dbt_utils.type_string() }}) as _DBT_SOURCE_RELATION
           , e.timestamp::DATE                                                                         AS date
           , TRIM(e.user_id)                                                                           AS server_id
           , COALESCE(TRIM(e.user_actual_id), NULL)                                                    AS user_id
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
           , COUNT(e.timestamp)                                                                                AS num_events
           , context_user_agent
           , MAX(e.timestamp)                                                                          AS max_timestamp
           , MIN(e.timestamp)                                                                          AS min_timestamp
           , MAX(date_trunc('hour', e.uuid_ts) + interval '1 hour')                                                        AS max_uuid_ts
           , e.category
           , {{ dbt_utils.surrogate_key(['e.timestamp::date', 'e.user_actual_id', 'e.user_id', 'e.context_user_agent', 'lower(e.type)', 'e.category']) }}                       AS id
         FROM {{ source('mattermost2', 'event') }} e
              LEFT JOIN (
                          SELECT user_id, MIN(TIMESTAMP::DATE) AS MIN_DATE
                          FROM {{ source('mm_telemetry_prod', 'event') }}
                          GROUP BY 1
                        ) rudder
                        ON e.timestamp::date >= rudder.MIN_DATE
                        AND e.user_id = rudder.user_id
         WHERE rudder.user_id IS NULL
         AND e.timestamp::DATE < CURRENT_DATE 
         {% if is_incremental() %}

          AND e.timestamp > (SELECT MAX(max_timestamp) from {{this}} 
                where _dbt_source_relation = {{ dbt_utils.string_literal(source('mattermost2', 'event')) }})

         {% endif %}
         GROUP BY 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13, 17, 18
     ), 
     events2 AS (
         SELECT
             cast({{ dbt_utils.string_literal(source('mm_telemetry_prod', 'event')) }} as {{ dbt_utils.type_string() }}) as _DBT_SOURCE_RELATION
            ,  e.timestamp::DATE                                                                         AS date
           , TRIM(e.user_id)                                                                           AS server_id
           , COALESCE(TRIM(e.user_actual_id), NULL)                                                    AS user_id
           , min(e.user_actual_role)                                                                   AS user_role
           , CASE
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%iPhone%'    THEN 'iPhone'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%iPad%'      THEN 'iPad'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Android%'   THEN 'Android'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Electron/%' THEN 'Electron'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Edge/%'     THEN 'Edge'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Edg/%'      THEN 'Edge'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%MSIE%'      THEN 'IE'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Trident/%'  THEN 'IE'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Firefox/%'  THEN 'Firefox'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Chrome/%'   THEN 'Chrome'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Safari/%'   THEN 'Safari'
              ELSE 'Other'
              END                                                                                        AS browser
           , CASE
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%iPhone%'    THEN 'iPhone'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%iPad%'      THEN 'iPad'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%CrOS%'      THEN 'Chrome OS'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Android%'   THEN 'Android'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Macintosh%' THEN 'Mac'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Windows%'   THEN 'Windows'
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Linux%'     THEN 'Linux'
              ELSE 'Other'
              END                                                                                      AS os
           , CASE
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Electron/%'
                                                        THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'Mattermost/', 2), ' ', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Edge/%'    THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'Edge/', 2), ' ', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Edg/%'     THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'Edg/', 2), ' ', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Trident/%' THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'Trident/', 2), ' ', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Chrome/%'  THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'Chrome/', 2), ' ', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Firefox/%' THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'Firefox/', 2), ' ', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Safari/%'  THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'Version/', 2), ' ', 1)
              ELSE 'Other'
              END                                                                                      AS version
           , CASE
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%iPhone%'    THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'iPhone OS ', 2), ' ', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%iPad%'      THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'CPU OS ', 2), ' ', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%CrOS%'      THEN split_part(
                split_part(split_part(coalesce(context_useragent, context_user_agent), 'CrOS ', 2), ')', 1), 'CS', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Android%'
                                                   THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'Android ', 2), ';', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Macintosh%'
                                                   THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'Mac OS X ', 2), ')', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Windows%'   THEN split_part(
                split_part(split_part(coalesce(context_useragent, context_user_agent), 'Windows ', 2), ')', 1), ';', 1)
              WHEN coalesce(context_useragent, context_user_agent) LIKE '%Linux%'     THEN split_part(split_part(coalesce(context_useragent, context_user_agent), 'Linux ', 2), ')', 1)
              ELSE 'Unknown'
              END                                                                                      AS os_version
           , LOWER(e.type)                                                                             AS event_name
           , CASE WHEN LOWER(coalesce(e.context_useragent, context_user_agent)) LIKE '%electron%' THEN 'desktop' ELSE 'web_app' END AS event_type
           , COUNT(e.timestamp)                                                                                  AS num_events
           , coalesce(context_useragent, context_user_agent)                                                                         AS context_user_agent
           , MAX(e.timestamp)                                                                          AS max_timestamp
           , MIN(e.timestamp)                                                                          AS min_timestamp
           , MAX(NULL::TIMESTAMP)                                                                       AS max_uuid_ts
           , e.category
           , {{ dbt_utils.surrogate_key(['e.timestamp::date', 'e.user_actual_id', 'e.user_id', 'coalesce(e.context_useragent, context_user_agent)', 'lower(e.type)', 'e.category']) }}                       AS id
         FROM {{ source('mm_telemetry_prod', 'event') }} e
         WHERE e.timestamp::DATE < CURRENT_DATE
         {% if is_incremental() %}

          AND timestamp > (SELECT MAX(max_timestamp) from {{this}} 
                where _dbt_source_relation = {{ dbt_utils.string_literal(source('mm_telemetry_prod', 'event')) }})

         {% endif %}
         GROUP BY 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13, 17, 18
     ), 
     all_events AS (
         SELECT *
         FROM mobile_events
         UNION ALL
         SELECT *
         FROM mobile_events2
         UNION ALL
         SELECT *
         FROM events
         UNION ALL
         SELECT *
         FROM events2
     ),

     all_events_chronological   AS (
         SELECT
           E._DBT_SOURCE_RELATION 
           ,  e.date
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
           , min(min_timestamp)                                                                       AS min_timestamp
         FROM all_events                  e
              LEFT JOIN {{ ref('events_registry') }} r
                   ON e.event_name = r.event_name
                   AND e.category = r.event_category

         GROUP BY 1, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14, 19
     ),

     user_events_by_date AS  (
       SELECT 
          DATE
        , CASE WHEN SERVER_ID IS NULL THEN 
            MAX(SERVER_ID) OVER (PARTITION BY DATE, USER_ID ORDER BY MIN_TIMESTAMP ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            ELSE SERVER_ID END                                                                                    AS SERVER_ID
        , CASE WHEN USER_ID IS NULL THEN 
            COALESCE(MAX(USER_ID) OVER (PARTITION BY DATE, SERVER_ID, CONTEXT_USER_AGENT ORDER BY MIN_TIMESTAMP ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), NULL)
            ELSE USER_ID END                                                                                      AS USER_ID
        , EVENT_TYPE
        , CASE WHEN USER_ROLE IS NULL THEN
            MAX(USER_ROLE) OVER (PARTITION BY USER_ID ORDER BY MIN_TIMESTAMP ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            ELSE USER_ROLE END                                                                                    AS USER_ROLE
        , CASE WHEN USER_ROLE IS NULL THEN
            CASE WHEN SPLIT_PART(MAX(USER_ROLE) OVER (PARTITION BY USER_ID ORDER BY MIN_TIMESTAMP ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), ',', 1) = 'system_admin'
              THEN TRUE ELSE FALSE END
            ELSE SYSTEM_ADMIN END                                                                                 AS SYSTEM_ADMIN                              
        , CASE WHEN USER_ROLE IS NULL THEN
            CASE WHEN SPLIT_PART(MAX(USER_ROLE) OVER (PARTITION BY USER_ID ORDER BY MIN_TIMESTAMP ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), ',', 1) = 'system_user'
              THEN TRUE ELSE FALSE END
            ELSE SYSTEM_USER END                                                                                  AS SYSTEM_USER
        , OS
        , BROWSER
        , BROWSER_VERSION
        , OS_VERSION
        , EVENT_ID
        , EVENT_NAME
        , TOTAL_EVENTS
        , DESKTOP_EVENTS
        , WEB_APP_EVENTS
        , MOBILE_EVENTS
        , CONTEXT_USER_AGENT
        , _DBT_SOURCE_RELATION
        , MAX_TIMESTAMP
        , MIN_TIMESTAMP
        , {{ dbt_utils.surrogate_key(['DATE', 
                                    'CASE WHEN USER_ID IS NULL THEN 
                                        COALESCE(MAX(USER_ID) OVER (PARTITION BY DATE, SERVER_ID ORDER BY MIN_TIMESTAMP ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), UUID_STRING())
                                        ELSE USER_ID END',
                                    'CASE WHEN SERVER_ID IS NULL THEN 
                                        MAX(SERVER_ID) OVER (PARTITION BY DATE, USER_ID ORDER BY MIN_TIMESTAMP ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
                                        ELSE SERVER_ID END',
                                    'CONTEXT_USER_AGENT', 
                                    'EVENT_NAME', 'OS', 'BROWSER_VERSION', 'OS_VERSION', 'EVENT_ID', 
                                    'MIN_TIMESTAMP', 'MAX_TIMESTAMP']) }}                                         AS ID
        , NULL                                        AS chronological_sequence
        , NULL AS seconds_after_prev_event
        , NULL AS UPDATED_AT
       FROM all_events_chronological
     )
SELECT *
FROM user_events_by_date