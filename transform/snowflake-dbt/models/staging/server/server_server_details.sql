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
    WHERE server.timestamp::DATE <= CURRENT_DATE - INTERVAL '1 DAY'
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND server.timestamp::DATE > (SELECT MAX(date) FROM {{ this }})

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
           , l.expire_date
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
     server_server_details AS (
         SELECT
             s.timestamp::DATE                    AS date
           , s.user_id                            AS server_id
           , s.version
           , MAX(s.context_library_version)       AS context_library_version
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
           , MAX(license.master_account_sfid)     AS master_account_sfid
           , MAX(license.account_sfid)            AS account_sfid
           , MAX(license.license_id)              AS license_id1
           , CASE WHEN MAX(license.license_id) = MIN(license.license_id) THEN MIN(NULL)
               ELSE MIN(license.license_id) END   AS license_id2
           , MAX(license.license_email)           AS license_email
           , MAX(license.contact_sfid)            AS license_contact_sfid
         FROM {{ source('mattermost2', 'server') }} s
              JOIN max_timestamp mt
                   ON s.user_id = mt.user_id
                       AND s.timestamp = mt.max_timestamp
              LEFT JOIN license
                        ON s.user_id = license.server_id
                            AND s.timestamp::date >= license.start_date
                            AND s.timestamp::date <= license.expire_date
         GROUP BY 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
     )
SELECT *
FROM server_server_details