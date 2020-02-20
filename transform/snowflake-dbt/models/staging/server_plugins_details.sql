{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp            AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'plugins') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_plugins_details AS (
         SELECT
             p.timestamp::DATE                  AS date
           , p.user_id                          AS server_id
           , MAX(active_backend_plugins)        AS active_backend_plugins
           , MAX(active_plugins)                AS active_plugins
           , MAX(active_webapp_plugins)         AS active_webapp_plugins
           , MAX(disabled_backend_plugins)      AS disabled_backend_plugins
           , MAX(disabled_plugins)              AS disabled_plugins
           , MAX(disabled_webapp_plugins)       AS disabled_webapp_plugins
           , MAX(enabled_backend_plugins)       AS enabled_backend_plugins
           , MAX(enabled_plugins)               AS enabled_plugins
           , MAX(enabled_webapp_plugins)        AS enabled_webapp_plugins
           , MAX(inactive_backend_plugins)      AS inactive_backend_plugins
           , MAX(inactive_plugins)              AS inactive_plugins
           , MAX(inactive_webapp_plugins)       AS inactive_webapp_plugins
           , MAX(plugins_with_broken_manifests) AS plugins_with_broken_manifests
           , MAX(plugins_with_settings)         AS plugins_with_settings
         FROM {{ source('mattermost2', 'plugins') }} p
              JOIN max_timestamp      mt
                   ON p.user_id = mt.user_id
                       AND p.timestamp = mt.max_timestamp
         GROUP BY 1, 2)
SELECT *
FROM server_plugins_details;