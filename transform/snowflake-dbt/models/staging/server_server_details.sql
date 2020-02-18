{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp              AS (
    SELECT
        server.timestamp::DATE AS date
      , server.user_id
      , MAX(server.timestamp)  AS max_timestamp
      , COUNT(server.user_id)  AS occurrences
    FROM {{ source('mattermost2', 'server') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE server.timestamp::DATE > (SELECT MAX(timestamp::date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
    license                 AS (
            SELECT
                license.timestamp::DATE AS license_date
              , license.user_id
              , license.license_id
              , license_overview.account_sfid
            FROM {{ source('mattermost2', 'license') }}
                  JOIN {{ ref('license_overview') }} 
                      ON license.license_id = license_overview.licenseid
                      AND license_overview.expiresat::DATE >= license.timestamp::DATE
                      AND license_overview.issuedat::DATE <= license.timestamp::DATE
            GROUP BY 1, 2, 3, 4
        ),
     server_server_details AS (
         SELECT
             s.timestamp::DATE                    AS date
           , s.user_id                            AS server_id
           , s.version
           , s.context_library_version
           , s.edition
           , s.system_admins
           , s.operating_system
           , s.database_type
           , s.event
           , s.event_text
           , s.sent_at
           , s.received_at
           , s.timestamp
           , s.original_timestamp
           , mt.occurrences
           , MAX(license.account_sfid)            AS account_sfid
           , MAX(license.license_id)              AS license_id1
           , CASE WHEN MAX(license.license_id) = MIN(license.license_id) THEN MIN(NULL)
               ELSE MIN(license.license_id) END   AS license_id2
         FROM {{ source('mattermost2', 'server') }} s
              JOIN max_timestamp mt
                   ON s.user_id = mt.user_id
                       AND s.timestamp = mt.max_timestamp
              LEFT JOIN license
                        ON s.id = license.user_id
                          AND s.timestamp::DATE = license.license_date
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
     )
SELECT *
FROM server_server_details