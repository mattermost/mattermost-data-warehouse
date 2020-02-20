{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp              AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
      , COUNT(user_id)  AS occurrences
    FROM {{ source('mattermost2', 'license') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::DATE > (SELECT MAX(timestamp::date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_license_details AS (
         SELECT
             l.timestamp::DATE                        AS date
           , l.license_id
           , MAX(_start)                              AS _start
           , MAX(edition)                             AS edition
           , MAX(expire)                              AS expire
           , MAX(feature_cluster)                     AS feature_cluster
           , MAX(feature_compliance)                  AS feature_compliance
           , MAX(feature_custom_brand)                AS feature_custom_brand
           , MAX(feature_custom_permissions_schemes)  AS feature_custom_permissions_schemes
           , MAX(feature_data_retention)              AS feature_data_retention
           , MAX(feature_elastic_search)              AS feature_elastic_search
           , MAX(feature_email_notification_contents) AS feature_email_notification_contents
           , MAX(feature_future)                      AS feature_future
           , MAX(feature_google)                      AS feature_google
           , MAX(feature_guest_accounts)              AS feature_guest_accounts
           , MAX(feature_guest_accounts_permissions)  AS feature_guest_accounts_permissions
           , MAX(feature_id_loaded)                   AS feature_id_loaded
           , MAX(feature_ldap)                        AS feature_ldap
           , MAX(feature_ldap_groups)                 AS feature_ldap_groups
           , MAX(feature_lock_teammate_name_display)  AS feature_lock_teammate_name_display
           , MAX(feature_message_export)              AS feature_message_export
           , MAX(feature_metrics)                     AS feature_metrics
           , MAX(feature_mfa)                         AS feature_mfa
           , MAX(feature_mhpns)                       AS feature_mhpns
           , MAX(feature_office365)                   AS feature_office365
           , MAX(feature_password)                    AS feature_password
           , MAX(feature_saml)                        AS feature_saml
           , MAX(issued)                              AS issued
           , MAX(users)                               AS users
         FROM {{ source('mattermost2', 'license') }} l
              JOIN max_timestamp      mt
                   ON l.license_id = mt.license_id
                       AND l.timestamp = mt.max_timestamp
         GROUP BY 1, 2)
SELECT *
FROM server_license_details