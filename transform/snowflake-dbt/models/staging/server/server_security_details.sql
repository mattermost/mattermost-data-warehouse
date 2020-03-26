{{config({
    "materialized": 'incremental',
    "schema": "staging"
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
      , sec.timestamp
      , COUNT(sec.location) over (partition by sec.id, sec.date, sec.hour, sec.active_user_count, sec.ip_address, sec.location) as location_count
    FROM {{ ref('security') }} sec
         LEFT JOIN {{ ref('excludable_servers') }} es
                   ON sec.id = es.server_id
    WHERE es.server_id IS NULL
      AND sec.dev_build = 0
      AND sec.ran_tests = 0
      AND sec.version LIKE '_.%._._.%._'
      AND sec.ip_address <> '194.30.0.184'
      AND sec.user_count >= sec.active_user_count
      AND NULLIF(sec.id, '') IS NOT NULL
      AND sec.date <= CURRENT_DATE - INTERVAL '1 DAY'
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
           , s.active_user_count
           , MAX(s.user_count)      AS user_count
           , MAX(s.version)         AS version
           , s.dev_build
           , MAX(s.db_type)         AS db_type
           , s.os_type
           , s.ran_tests
           , MAX(m.ip_count)        AS ip_count
           , MAX(m.occurrences)     AS occurrences
         FROM security      s
              JOIN max_hour m
                   ON COALESCE(NULLIF(s.id, ''), s.ip_address) = m.id
                       AND s.date = m.date
                       AND s.active_user_count = m.max_active_users
                       AND s.timestamp = m.max_timestamp
                       AND s.ip_address = m.max_ip
         GROUP BY 1, 2, 3, 4, 5, 7, 10, 12, 13
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
           , l.feature_cluster
           , l.feature_compliance
           , l.feature_custom_brand
           , l.feature_custom_permissions_schemes
           , l.feature_data_retention
           , l.feature_elastic_search
           , l.feature_email_notification_contents
           , l.feature_future
           , l.feature_google
           , l.feature_guest_accounts
           , l.feature_guest_accounts_permissions
           , l.feature_id_loaded
           , l.feature_ldap
           , l.feature_ldap_groups
           , l.feature_lock_teammate_name_display
           , l.feature_message_export
           , l.feature_metrics           
           , l.feature_mfa
           , l.feature_mhpns
           , l.feature_office365
           , l.feature_password
           , l.feature_saml
         FROM {{ ref('licenses') }} l
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
         , 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40
     ),
     server_security_details    AS (
         SELECT
             s.id                                 AS server_id
           , s.date
           , s.hour
           , s.grouping
           , s.ip_address
           , s.location
           , s.active_user_count
           , s.user_count
           , s.version
           , MAX(s.db_type)                       AS db_type
           , s.os_type
           , MAX(license.master_account_sfid)     AS master_account_sfid
           , MAX(license.account_sfid)            AS account_sfid
           , MAX(license.license_id)              AS license_id1
           , CASE WHEN MAX(license.license_id) = MIN(license.license_id) THEN MIN(NULL)
               ELSE MIN(license.license_id) END   AS license_id2
           , MAX(license.license_email)           AS license_email
           , MAX(license.contact_sfid)            AS license_contact_sfid
           , s.ip_count
           , s.occurrences
         FROM server_details s
              LEFT JOIN license
                        ON s.id = license.server_id
                            AND s.date >= license.issued_date
                            AND s.date <= license.expire_date
                            AND CASE WHEN l.has_trial_and_non_trial AND NOT l.trial THEN TRUE
                                  WHEN NOT l.has_trial_and_non_trial AND l.trial THEN TRUE
                                  WHEN NOT l.has_trial_and_non_trial AND NOT l.trial THEN TRUE
                                  ELSE FALSE END
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 18, 19
     )
SELECT *
FROM server_security_details
