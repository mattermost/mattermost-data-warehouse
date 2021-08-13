{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH license_daily_details as (
         SELECT 
             l.date
           , l.license_id
           , l.customer_id
           , l.server_id
           , l.company
           , l.edition
           , l.trial
           , l.issued_date
           , l.start_date
           , l.server_expire_date_join                                               AS expire_date
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
           , MAX(l.timestamp)                                                        AS timestamp
           , l.installation_id
           , l.feature_advanced_logging
           , l.feature_cloud
         FROM {{ ref('licenses') }} l
         WHERE l.date <= CURRENT_DATE
         AND l.date <= l.server_expire_date_join
         AND l.date >= l.start_date
         AND CASE WHEN l.has_trial_and_non_trial AND NOT l.trial THEN TRUE
              WHEN NOT l.has_trial_and_non_trial AND l.trial THEN TRUE
              WHEN NOT l.has_trial_and_non_trial AND NOT l.trial THEN TRUE
              ELSE FALSE END
         {% if is_incremental() %}

         AND l.date >= (SELECT MAX(date) FROM {{this}}) - INTERVAL '1 DAY'

         {% endif %}
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
         , 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30
         , 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 44
         , 45, 46
     ),

     server_license_details AS (
         SELECT
             d.date
           , d.server_id
           , d.license_id
           , MAX(start_date)                          AS start_date
           , MAX(edition)                             AS edition
           , MIN(expire_date)                         AS expire_date
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
           , MAX(issued_date)                         AS issued_date
           , MAX(users)                               AS users
           , {{ dbt_utils.surrogate_key(['d.date','d.server_id'])}} AS id
           , MAX(installation_id)                     AS installation_id
           , MAX(feature_advanced_logging)            AS feature_advanced_logging
           , MAX(feature_cloud)                       AS feature_cloud
         FROM license_daily_details d
         WHERE d.date <= CURRENT_DATE
         AND d.server_id is not null
         {% if is_incremental() %}
         
         AND d.date >= (SELECT MAX(date) FROM {{this}}) - INTERVAL '1 DAY'

         {% endif %}
         GROUP BY 1, 2, 3
         HAVING MIN(d.expire_date) >= d.date)
SELECT *
FROM server_license_details