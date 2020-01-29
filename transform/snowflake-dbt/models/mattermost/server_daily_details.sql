{{config({
    "materialized": 'incremental',
    "schema": "mattermost"
  })
}}

WITH security AS (
    SELECT 
        sec.id,
        sec.date,
        sec.hour,
        sec.grouping, 
        sec.ip_address,
        sec.location,
        sec.active_user_count,
        sec.user_count,
        sec.version,
        sec.dev_build,
        sec.db_type,
        sec.os_type,
        sec.ran_tests
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
        and date > (SELECT MAX(day) FROM {{ this }})

    {% endif %}
      ),
     max_users AS (
         SELECT 
            sec.date,
            coalesce(nullif(sec.id, ''), sec.ip_address) AS id,
            max(sec.active_user_count)                   AS max_active_users,
            count(sec.id)                                AS occurrences
         FROM security sec
         GROUP BY 1, 2
         ),
    max_hour AS (
         SELECT 
            s.date,
            coalesce(nullif(s.id, ''), s.ip_address) AS id,
            m.max_active_users,
            m.occurrences,
            max(s.hour)                              AS max_hour,
            max(ip_address)                          AS max_ip,
            max(version)                             AS max_version
         FROM security s
                  JOIN max_users m
                       ON coalesce(nullif(s.id, ''), s.ip_address) = m.id
                           AND s.date = m.date
                           AND s.active_user_count = m.max_active_users
         GROUP BY 1, 2, 3, 4
         ),
    server_security_details AS (
        SELECT 
            s.id,
            s.date,
            s.hour,
            s.grouping, 
            s.ip_address,
            s.location,
            s.active_user_count,
            s.user_count,
            s.version,
            s.dev_build,
            s.db_type,
            s.os_type,
            s.ran_tests
        FROM security s
                JOIN max_hour m
                    ON coalesce(nullif(s.id, ''), s.ip_address) = m.id
                        AND s.date = m.date
                        AND s.active_user_count = m.max_active_users
                        AND s.hour = m.max_hour
                        AND s.ip_address = m.max_ip
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    ),
    server_daily_details AS (
        SELECT 
            s.id,
            s.date,
            s.hour,
            s.grouping, 
            s.ip_address,
            s.location,
            s.active_user_count,
            s.user_count,
            s.version,
            s.dev_build,
            s.db_type,
            s.os_type,
            s.ran_tests 
            license_overview.account_sfid, 
            license.license_id
        FROM server_security_details
        LEFT JOIN {{ source('mattermost2', 'license') }} ON license.user_id = server.user_id
        LEFT JOIN {{ ref('license_overview') }} ON license_overview.licenseid = license.license_id
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    )

SELECT * FROM server_daily_details

