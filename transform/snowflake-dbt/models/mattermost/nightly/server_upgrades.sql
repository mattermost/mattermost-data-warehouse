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
      , edition                                                  AS current_edition
      , lag(edition) OVER (PARTITION BY server_id ORDER BY date) AS prev_edition
    FROM {{ ref('server_daily_details') }}
    WHERE NOT tracking_disabled
                        ),
     server_upgrades AS (
         SELECT
             date
           , server_id
           , substr(prev_version, 0, length(current_version)) AS prev_version
           , substr(current_version, 0, length(prev_version)) AS current_version
           , prev_edition
           , current_edition
         FROM upgrade
         WHERE (substr(current_version, 0, length(prev_version)) >
                coalesce(substr(prev_version, 0, length(current_version)),
                         substr(current_version, 0, length(prev_version)))
             OR (current_edition = 'true' AND coalesce(prev_edition, 'true') <> current_edition))
         {% if is_incremental() %}
         
         AND date > (SELECT MAX(date) FROM {{this}} )

         {% endif %}
     )
SELECT *
FROM server_upgrades
