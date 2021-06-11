{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"union",
    "unique_key":"id"
  })
}}

WITH daily_user_agent_events AS (
    SELECT
        events.timestamp::DATE                                                        AS date
      , COALESCE(user_agent.browser, CASE WHEN events._dbt_source_relation2 IN ('"ANALYTICS".EVENTS.MOBILE_EVENTS') THEN 'Mobile App' ELSE NULL END) AS browser
      , COALESCE(user_agent.browser_version, CASE WHEN events._dbt_source_relation2 IN ('"ANALYTICS".EVENTS.MOBILE_EVENTS') THEN events.context_app_version ELSE NULL END) AS browser_version
      , COALESCE(user_agent.operating_system, CASE WHEN events._dbt_source_relation2 IN ('"ANALYTICS".EVENTS.MOBILE_EVENTS') THEN events.context_os_name ELSE NULL END) AS operating_system
      , COALESCE(user_agent.os_version, CASE WHEN events._dbt_source_relation2 IN ('"ANALYTICS".EVENTS.MOBILE_EVENTS') THEN events.context_os_version ELSE NULL END) AS os_version
      , COALESCE(user_agent.device_type, CASE WHEN events._dbt_source_relation2 IN ('"ANALYTICS".EVENTS.MOBILE_EVENTS') THEN events.context_device_model ELSE NULL END) AS device_type
      , COALESCE(user_agent.device_brand, CASE WHEN events._dbt_source_relation2 IN ('"ANALYTICS".EVENTS.MOBILE_EVENTS') THEN events.context_device_manufacturer ELSE NULL END) AS device_brand
      , COALESCE(user_agent.device_model, CASE WHEN events._dbt_source_relation2 IN ('"ANALYTICS".EVENTS.MOBILE_EVENTS') THEN events.context_device_model ELSE NULL END) AS device_model
      , {{ dbt_utils.surrogate_key(['events.timestamp::DATE'
      , 'COALESCE(user_agent.browser, CASE WHEN events._dbt_source_relation2 IN (\'"ANALYTICS".EVENTS.MOBILE_EVENTS\') THEN \'Mobile App\' ELSE NULL END)'
      , 'COALESCE(user_agent.browser_version, CASE WHEN events._dbt_source_relation2 IN (\'"ANALYTICS".EVENTS.MOBILE_EVENTS\') THEN events.context_app_version ELSE NULL END)'
      , 'COALESCE(user_agent.operating_system, CASE WHEN events._dbt_source_relation2 IN (\'"ANALYTICS".EVENTS.MOBILE_EVENTS\') THEN events.context_os_name ELSE NULL END)'
      , 'COALESCE(user_agent.os_version, CASE WHEN events._dbt_source_relation2 IN (\'"ANALYTICS".EVENTS.MOBILE_EVENTS\') THEN events.context_os_version ELSE NULL END)'
      , 'COALESCE(user_agent.device_type, CASE WHEN events._dbt_source_relation2 IN (\'"ANALYTICS".EVENTS.MOBILE_EVENTS\') THEN events.context_device_model ELSE NULL END)'
      , 'COALESCE(user_agent.device_brand, CASE WHEN events._dbt_source_relation2 IN (\'"ANALYTICS".EVENTS.MOBILE_EVENTS\') THEN events.context_device_manufacturer ELSE NULL END)'
      , 'COALESCE(user_agent.device_model, CASE WHEN events._dbt_source_relation2 IN (\'"ANALYTICS".EVENTS.MOBILE_EVENTS\') THEN events.context_device_model ELSE NULL END)'])}}                   AS id
      , COUNT(DISTINCT COALESCE(events.user_id, events.context_traits_server, events.context_server)) AS instance_count
      , COUNT(DISTINCT COALESCE(events.user_actual_id, events.anonymous_id))          AS user_count
      , COUNT(events.id)                   AS events
      , MAX(events.timestamp) AS last_updated
    FROM {{ ref('user_events_telemetry') }}        events
         JOIN ANALYTICS.web.user_agent_registry user_agent
              ON NULLIF(COALESCE(events.context_user_agent, events.context_useragent), '') = user_agent.context_useragent
              AND user_agent.context_useragent != 'unknown'
    {% if is_incremental() %}
    WHERE events.timestamp::TIMESTAMP >= (SELECT MAX(last_updated)::TIMESTAMP - INTERVAL '6 HOURS' FROM {{this}})
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
                   )
SELECT *
FROM daily_user_agent_events