{{config({
    "materialized": 'incremental',
    "schema": "mattermost"
  })
}}

WITH security                AS (
    SELECT
        sec.id
      , sec.date
      , sec.hour
      , sec.grouping
      , sec.ip_address
      , sec.location
      , sec.active_user_count
      , sec.user_count
      , sec.version
      , sec.dev_build
      , sec.db_type
      , sec.os_type
      , sec.ran_tests
      , count(sec.location) over (partition by sec.id, sec.date, sec.hour, sec.active_user_count, sec.ip_address, sec.location) as location_count
    FROM {{ ref('security') }} sec
         LEFT JOIN {{ ref('excludable_servers') }} es
                   ON sec.id = es.server_id
    WHERE es.server_id IS NULL
      AND sec.dev_build = 0
      AND sec.ran_tests = 0
      AND sec.version LIKE '_.%._._.%._'
      AND sec.ip_address <> '194.30.0.184'
      AND sec.user_count >= sec.active_user_count
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
),
     max_users               AS (
         SELECT
             sec.date
           , COALESCE(NULLIF(sec.id, ''), sec.ip_address) AS id
           , MAX(sec.active_user_count)                   AS max_active_users
           , COUNT(sec.id)                                AS occurrences
         FROM security sec
         GROUP BY 1, 2
     ),
     max_hour                AS (
         SELECT
             s.date
           , COALESCE(NULLIF(s.id, ''), s.ip_address) AS id
           , m.max_active_users
           , m.occurrences
           , MAX(s.hour)                              AS max_hour
           , MAX(ip_address)                          AS max_ip
           , MAX(version)                             AS max_version
           , MAX(s.location_count)                    AS max_location_count
         FROM security       s
              JOIN max_users m
                   ON COALESCE(NULLIF(s.id, ''), s.ip_address) = m.id
                       AND s.date = m.date
                       AND s.active_user_count = m.max_active_users
         GROUP BY 1, 2, 3, 4
     ),
     server_security_details AS (
         SELECT
             s.id
           , s.date
           , s.hour
           , s.grouping
           , s.ip_address
           , MAX(CASE WHEN m.max_location_count = s.location_count THEN s.location 
                    ELSE NULL END) AS location
           , s.active_user_count
           , MAX(s.user_count)     AS user_count
           , MAX(s.version)        AS version
           , s.dev_build
           , s.db_type
           , s.os_type
           , s.ran_tests
         FROM security      s
              JOIN max_hour m
                   ON COALESCE(NULLIF(s.id, ''), s.ip_address) = m.id
                       AND s.date = m.date
                       AND s.active_user_count = m.max_active_users
                       AND s.hour = m.max_hour
                       AND s.ip_address = m.max_ip
         GROUP BY 1, 2, 3, 4, 5, 7, 10, 11, 12, 13
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
     server_daily_details    AS (
         SELECT
             s.id
           , s.date
           , s.hour
           , s.grouping
           , s.ip_address
           , s.location
           , s.active_user_count
           , s.user_count
           , s.version
           , s.db_type
           , s.os_type
           , license.account_sfid
           , license.license_id
         FROM server_security_details s
              LEFT JOIN license
                        ON s.id = license.user_id
                            AND s.date = license.license_date
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
     )
SELECT *
FROM server_daily_details
