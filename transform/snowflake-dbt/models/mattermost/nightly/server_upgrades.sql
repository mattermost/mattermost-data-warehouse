{{config({
    "materialized": 'incremental',
    "schema": "mattermost"
  })
}}

WITH upgrade         AS (
    SELECT
        date
      , server_id
      , lag(version) OVER (PARTITION BY server_id ORDER BY date) AS prev_version
      , version                                                  AS current_version
    FROM {{ ref('server_daily_details') }}
    WHERE NOT tracking_disabled
                        ),
     server_upgrades AS (
         SELECT *
         FROM upgrade
         WHERE substr(current_version, 0, length(prev_version)) > substr(prev_version, 0, length(current_version))
         {% if is_incremental() %}

         AND date > (SELECT MAX(date) FROM {{this}})

         {% endif %}
     )
SELECT *
FROM server_upgrades
