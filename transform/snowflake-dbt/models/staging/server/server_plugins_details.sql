{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp        AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'plugins') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'plugins') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_plugins_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , MAX(COALESCE(s.active_backend_plugins, NULL ))        AS active_backend_plugins
           , MAX(COALESCE(s.active_plugins, NULL ))                AS active_plugins
           , MAX(COALESCE(s.active_webapp_plugins, NULL ))         AS active_webapp_plugins
           , MAX(COALESCE(r.disabled_backend_plugins, s.disabled_backend_plugins))      AS disabled_backend_plugins
           , MAX(COALESCE(r.disabled_plugins, s.disabled_plugins))              AS disabled_plugins
           , MAX(COALESCE(r.disabled_webapp_plugins, s.disabled_webapp_plugins))       AS disabled_webapp_plugins
           , MAX(COALESCE(r.enabled_backend_plugins, s.enabled_backend_plugins))       AS enabled_backend_plugins
           , MAX(COALESCE(r.enabled_plugins, s.enabled_plugins))               AS enabled_plugins
           , MAX(COALESCE(r.enabled_webapp_plugins, s.enabled_webapp_plugins))        AS enabled_webapp_plugins
           , MAX(COALESCE(s.inactive_backend_plugins, NULL))      AS inactive_backend_plugins
           , MAX(COALESCE(s.inactive_plugins, NULL))              AS inactive_plugins
           , MAX(COALESCE(s.inactive_webapp_plugins, NULL))       AS inactive_webapp_plugins
           , MAX(COALESCE(r.plugins_with_broken_manifests, s.plugins_with_broken_manifests)) AS plugins_with_broken_manifests
           , MAX(COALESCE(r.plugins_with_settings, s.plugins_with_settings))         AS plugins_with_settings           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'plugins') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'plugins') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 17, 18
         )
SELECT *
FROM server_plugins_details