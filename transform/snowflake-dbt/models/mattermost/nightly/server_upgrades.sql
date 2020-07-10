{{config({
    "materialized": 'incremental',
    "schema": "mattermost"
  })
}}

WITH upgrade         AS (
    SELECT
        date
      , server_id
      , max(account_sfid) OVER (PARTITION BY server_id)          AS account_sfid
      , license_id1                                              AS license_id
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
           , account_sfid
           , license_id
           , regexp_substr(prev_version, '^[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,2}') AS prev_version
           , regexp_substr(current_version, '^[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,2}') AS current_version
           , prev_edition
           , current_edition
         FROM upgrade
         WHERE (regexp_substr(current_version, '^[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,2}') >
                coalesce(regexp_substr(prev_version, '^[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,2}'),
                         regexp_substr(current_version, '^[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,2}'))
             OR (current_edition = 'true' AND coalesce(prev_edition, 'true') = 'false'))
         {% if is_incremental() %}

         AND date > (SELECT MAX(date) FROM {{this}} )

         {% endif %}
     )
SELECT *
FROM server_upgrades
