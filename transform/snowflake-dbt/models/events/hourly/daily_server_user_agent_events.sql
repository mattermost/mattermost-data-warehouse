{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"union",
    "unique_key":"id"
  })
}}

WITH daily_server_user_agent_events AS (
    SELECT
        events.timestamp::DATE                                                        AS date
      , COALESCE(events.user_id, events.context_traits_server, events.context_server) AS server_id
      , CASE
            WHEN SPLIT_PART(events._dbt_source_relation2, '.', 3) IN
                 ('SEGMENT_WEBAPP_EVENTS', 'RUDDER_WEBAPP_EVENTS') AND
                 LOWER(COALESCE(events.context_user_agent, events.context_useragent)) LIKE '%electron%'         THEN 'Desktop'
            WHEN SPLIT_PART(events._dbt_source_relation2, '.', 3) IN
                 ('SEGMENT_WEBAPP_EVENTS', 'RUDDER_WEBAPP_EVENTS') AND
                 LOWER(COALESCE(events.context_user_agent, events.context_useragent)) NOT LIKE '%electron%'
                                                                                                                THEN 'WebApp'
            WHEN SPLIT_PART(events._dbt_source_relation2, '.', 3) IN ('SEGMENT_MOBILE_EVENTS', 'MOBILE_EVENTS')
                                                                                                                THEN 'Mobile'
            WHEN SPLIT_PART(events._dbt_source_relation2, '.', 3) IN ('PORTAL_EVENTS')
                                                                                                                THEN 'Customer Portal'
                                                                                                                ELSE 'WebApp' END AS event_source
      , user_agent.*
      , {{ dbt_utils.surrogate_key(['events.timestamp::DATE'
                                    , 'COALESCE(events.user_id, events.context_traits_server, events.context_server)'
                                    , 'CASE
                                        WHEN SPLIT_PART(events._dbt_source_relation2, \'.\', 3) IN
                                            (\'SEGMENT_WEBAPP_EVENTS\', \'RUDDER_WEBAPP_EVENTS\') AND
                                            LOWER(COALESCE(events.context_user_agent, events.context_useragent)) LIKE \'%electron%\'         THEN \'Desktop\'
                                        WHEN SPLIT_PART(events._dbt_source_relation2, \'.\', 3) IN
                                            (\'SEGMENT_WEBAPP_EVENTS\', \'RUDDER_WEBAPP_EVENTS\') AND
                                            LOWER(COALESCE(events.context_user_agent, events.context_useragent)) NOT LIKE \'%electron%\'
                                                                                                                                            THEN \'WebApp\'
                                        WHEN SPLIT_PART(events._dbt_source_relation2, \'.\', 3) IN (\'SEGMENT_MOBILE_EVENTS\', \'MOBILE_EVENTS\')
                                                                                                                                            THEN \'Mobile\'
                                        WHEN SPLIT_PART(events._dbt_source_relation2, \'.\', 3) IN (\'PORTAL_EVENTS\')
                                                                                                                                            THEN \'Customer Portal\'
                                                                                                                                            ELSE \'WebApp\' END'
                                    , 'user_agent.context_useragent'
                                    , 'user_agent.browser'
                                    , 'user_agent.browser_version'
                                    , 'user_agent.operating_system'
                                    , 'user_agent.os_version'
                                    , 'user_agent.device_type'
                                    , 'user_agent.device_brand'
                                    , 'user_agent.device_model'])}}                   AS id
      , COUNT(DISTINCT COALESCE(events.user_actual_id, events.anonymous_id))          AS user_count
      , COUNT(COALESCE(events.user_actual_id, events.anonymous_id))                   AS events
    FROM {{ ref('user_events_telemetry') }}        events
         JOIN ANALYTICS.mattermost.user_agent_registry user_agent
              ON COALESCE(events.context_user_agent, events.context_useragent) = user_agent.context_useragent
    WHERE COALESCE(events.user_id, events.context_traits_server, events.context_server) IS NOT NULL
    {% if is_incremental() %}
    AND events.timestamp::DATE >= (SELECT MAX(date) FROM {{this}})
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
                   )
SELECT *
FROM daily_server_user_agent_events