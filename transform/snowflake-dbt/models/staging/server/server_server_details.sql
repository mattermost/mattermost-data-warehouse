{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH server_details AS (
  SELECT
    COALESCE(s2.anonymous_id, s1.user_id)                            AS annonymous_id
  , COALESCE(s2.channel, ''::VARCHAR)                                AS channel
  , COALESCE(s2.context_ip, ''::VARCHAR)                             AS context_ip
  , COALESCE(s2.context_library_name, s1.context_library_name)       AS context_library_name
  , COALESCE(s2.context_library_version, s1.context_library_version) AS context_library_version
  , COALESCE(s2.database_type, s1.database_type)                     AS database_type
  , COALESCE(s2.database_version, s1.database_version)               AS database_version
  , COALESCE(s2.edition, s1.edition)                                 AS edition
  , COALESCE(s2.event, s1.event)                                     AS event
  , COALESCE(s2.event_text, s1.event_text)                           AS event_text
  , COALESCE(s2.id, s1.id)                                           AS id
  , COALESCE(s2.operating_system, s1.operating_system)               AS operating_system
  , COALESCE(s2.system_admins, s1.system_admins)                     AS system_admins
  , COALESCE(s2.timestamp, s1.timestamp)                             AS timestamp
  , COALESCE(s2.user_id, s1.user_id)                                 AS user_id
  , MAX(COALESCE(s2.uuid_ts, s1.uuid_ts))                            AS uuid_ts
  , COALESCE(s2.version, s1.version)                                 AS version
  , MAX(COALESCE(s2.sent_at, s1.sent_at))                            AS sent_at
  , MAX(COALESCE(s2.received_at, s1.received_at))                    AS received_at
FROM {{ source('mattermost2', 'server') }}                       s1
     FULL OUTER JOIN {{ source('mm_telemetry_prod', 'server') }} s2
                     ON s1.user_id = s2.user_id
                         AND s1.timestamp::DATE = s2.timestamp::DATE
WHERE COALESCE(s2.timestamp::date, s1.timestamp::date) <= CURRENT_DATE
{% if is_incremental() %}

AND COALESCE(s2.timestamp::date, s1.timestamp::date) >= (SELECT MAX(DATE-interval '1 day') FROM {{this}})

{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17
),
max_timestamp              AS (
    SELECT
        s1.timestamp::DATE         AS date
      , s1.user_id                 AS user_id
      , MAX(s1.timestamp)          AS max_timestamp
      , COUNT(s1.user_id)          AS occurrences
    FROM server_details s1
    WHERE s1.timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND s1.timestamp::DATE >= (SELECT MAX(DATE - INTERVAL '1 DAY') FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
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
           , l.server_expire_date_join   AS expire_date
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
           , l.has_trial_and_non_trial
           , l.trial
         FROM {{ ref('licenses') }} l
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
         , 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42
     ),
     server_server_details AS (
         SELECT
             s.timestamp::DATE                    AS date
           , s.user_id                            AS server_id
           , MAX(s.version)                       AS version
           , MAX(s.context_library_version)       AS context_library_version
           , s.edition
           , MAX(s.system_admins)                 AS system_admins
           , s.operating_system
           , s.database_type
           , s.event
           , s.event_text
           , MAX(s.sent_at)                       AS sent_at
           , MAX(s.received_at)                   AS received_at
           , s.timestamp
           , s.timestamp                          AS original_timestamp
           , mt.occurrences
           , MAX(license.master_account_sfid)     AS master_account_sfid
           , MAX(license.account_sfid)            AS account_sfid
           , MAX(license.license_id)              AS license_id1
           , CASE WHEN MAX(license.license_id) = MIN(license.license_id) THEN MIN(NULL)
               ELSE MIN(license.license_id) END   AS license_id2
           , MAX(license.license_email)           AS license_email
           , MAX(license.contact_sfid)            AS license_contact_sfid
           , {{ dbt_utils.surrogate_key('s.timestamp::date', 's.user_id') }} AS id
           , s.context_ip                         AS context_ip
           , MAX(s.database_version)              AS database_version
         FROM server_details s
              JOIN max_timestamp mt
                   ON s.user_id = mt.user_id
                       AND s.timestamp = mt.max_timestamp
              LEFT JOIN license
                        ON s.user_id = license.server_id
                            AND s.timestamp::date >= license.issued_date
                            AND s.timestamp::date <= license.expire_date
                            AND CASE WHEN license.has_trial_and_non_trial AND NOT license.trial THEN TRUE
                                  WHEN NOT license.has_trial_and_non_trial AND license.trial THEN TRUE
                                  WHEN NOT license.has_trial_and_non_trial AND NOT license.trial THEN TRUE
                                  ELSE FALSE END
        {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE s.timestamp::date >= (SELECT MAX(DATE-interval '1 day') FROM {{ this }})

         {% endif %}
         GROUP BY 1, 2, 5, 7, 8, 9, 10, 13, 14, 15, 22, 23
     )
SELECT *
FROM server_server_details