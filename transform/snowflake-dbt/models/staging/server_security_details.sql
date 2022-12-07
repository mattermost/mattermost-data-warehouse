{{config({
    "materialized": 'incremental',
    "schema": "staging",
    "unique_key":'id',
    "tags":'hourly'
  })
}}

WITH security                AS (
    SELECT
        sec.server_id as id
      , sec.date
      , sec.hour
      , sec.grouping
      , sec.ip_address
      , sec.location
      , sec.edition
      , sec.active_user_count
      , sec.user_count
      , sec.version
      , sec.dev_build
      , sec.db_type
      , sec.os_type
      , sec.ran_tests
      , sec.timestamp
      , COUNT(sec.location) over (partition by sec.id, sec.date, sec.hour, sec.active_user_count, sec.ip_address, sec.location) as location_count
    FROM {{ ref('security') }} sec
    WHERE sec.dev_build = 0
      AND sec.ran_tests = 0
      AND (regexp_substr(sec.version, '^[0-9]{1,2}\.{1}[0-9]{1,2}.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}$') is not null
      OR regexp_substr(sec.version, '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}$') is not null
      OR regexp_substr(sec.version, '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}(cloud(-|\.){1}|ee_live{1})') is not null)
      AND sec.ip_address <> '194.30.0.184'
      AND sec.user_count >= sec.active_user_count
      AND NULLIF(sec.server_id, '') IS NOT NULL
      AND sec.date <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
),
     max_users               AS (
         SELECT
             sec.date
           , COALESCE(NULLIF(sec.id, ''), sec.ip_address) AS id
           , MAX(sec.active_user_count)                   AS max_active_users
           , COUNT(sec.id)                                AS occurrences
           , COUNT(DISTINCT sec.ip_address)               AS ip_count
         FROM security sec
         GROUP BY 1, 2
     ),
     max_hour                AS (
         SELECT
             s.date
           , COALESCE(NULLIF(s.id, ''), s.ip_address) AS id
           , m.max_active_users
           , m.occurrences
           , MAX(s.timestamp)                         AS max_timestamp
           , MAX(ip_address)                          AS max_ip
           , MAX(version)                             AS max_version
           , MAX(s.location_count)                    AS max_location_count
           , MAX(m.ip_count)                          AS ip_count
         FROM security       s
              JOIN max_users m
                   ON COALESCE(NULLIF(s.id, ''), s.ip_address) = m.id
                       AND s.date = m.date
                       AND s.active_user_count = m.max_active_users
         GROUP BY 1, 2, 3, 4
     ),
     server_details AS (
         SELECT
             s.id
           , s.date
           , s.hour
           , s.grouping
           , s.ip_address
           , MAX(CASE WHEN m.max_location_count = s.location_count THEN s.location 
                    ELSE NULL END)  AS location
           , s.edition
           , s.active_user_count
           , MAX(s.user_count)      AS user_count
           , MAX(s.version)         AS version
           , s.dev_build
           , MAX(s.db_type)         AS db_type
           , s.os_type
           , s.ran_tests
           , MAX(m.ip_count)        AS ip_count
           , MAX(m.occurrences)     AS occurrences
           , s.timestamp
         FROM security      s
              JOIN max_hour m
                   ON COALESCE(NULLIF(s.id, ''), s.ip_address) = m.id
                       AND s.date = m.date
                       AND s.active_user_count = m.max_active_users
                       AND s.timestamp = m.max_timestamp
                       AND s.ip_address = m.max_ip
         GROUP BY 1, 2, 3, 4, 5, 7, 8, 11, 13, 14, 17
     ),
     license                 AS (
         SELECT
             l.license_id
           , l.server_id
           , l.customer_id
           , l.company
           , l.edition
           , l.issued_date
           , l.start_date
           , l.server_expire_date_join          AS expire_date
           , l.master_account_sfid
           , l.master_account_name
           , l.account_sfid
           , l.account_name
           , l.license_email
           , l.contact_sfid
           , l.contact_email
           , l.number
           , l.stripeid
           , l.users
           , l.has_trial_and_non_trial
           , l.trial
         FROM {{ ref('licenses') }} l
         WHERE EXISTS (
                        SELECT s.id FROM security s 
                        WHERE l.server_id = s.id
                      )
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
     ),
     server_security_details_tmp AS (
         SELECT
             s.id                                 AS server_id
           , s.date
           , s.hour
           , s.grouping
           , s.ip_address
           , s.location
           , s.edition
           , s.active_user_count
           , s.user_count
           , s.version
           , MAX(s.db_type)                       AS db_type
           , s.os_type
           , MAX(license.master_account_sfid)     AS master_account_sfid
           , MAX(license.account_sfid)            AS account_sfid
           , MAX(CASE WHEN license.has_trial_and_non_trial AND NOT license.trial THEN license.license_id
                 WHEN NOT license.has_trial_and_non_trial THEN license.license_id
                 ELSE NULL END)              AS license_id1
           , MAX(CASE WHEN license.has_trial_and_non_trial AND license.trial THEN license.license_id
                 ELSE NULL END)              AS license_id2
           , MAX(license.license_email)           AS license_email
           , MAX(license.contact_sfid)            AS license_contact_sfid
           , s.ip_count
           , s.occurrences
           , s.timestamp
           , {{ dbt_utils.surrogate_key(['s.id', 's.date'])}} AS id
         FROM server_details s
              LEFT JOIN license
                        ON s.id = license.server_id
                            AND s.date >= license.issued_date
                            AND s.date <= license.expire_date
        {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND s.date >= (SELECT MAX(date) FROM {{ this }})

         {% endif %}
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 19, 20, 21, 22
     ),
    server_security_details AS (
        -- Handle case where duplicate data exist for max entry.
        SELECT
            *
        FROM
            server_security_details_tmp
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY edition DESC) = 1
    )
SELECT *
FROM server_security_details
