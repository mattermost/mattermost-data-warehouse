{{config({
    "materialized": 'incremental',
    "schema": "mattermost",
    "unique_key": 'id'
  })
}}

WITH license_daily_details as (
         SELECT 
             l.date
           , l.license_id
           , l.customer_id
           , l.company
           , l.trial
           , l.issued_date
           , l.start_date
           , l.server_expire_date                                                AS expire_date
           , l.master_account_sfid
           , l.master_account_name
           , l.account_sfid
           , l.account_name
           , l.license_email
           , l.contact_sfid
           , l.contact_email
           , l.number
           , l.stripeid
           , MAX(l.edition)                                                          AS edition
           , MAX(l.users)                                                            AS users
           , MAX(l.feature_cluster)                                                  AS feature_cluster
           , MAX(l.feature_compliance)                                               AS feature_compliance
           , MAX(l.feature_custom_brand)                                             AS feature_custom_brand
           , MAX(l.feature_custom_permissions_schemes)                               AS feature_custom_permissions_schemes
           , MAX(l.feature_data_retention)                                           AS feature_data_retention
           , MAX(l.feature_elastic_search)                                           AS feature_elastic_search
           , MAX(l.feature_email_notification_contents)                              AS feature_email_notification_contents                        
           , MAX(l.feature_future)                                                   AS feature_future
           , MAX(l.feature_google)                                                   AS feature_google
           , MAX(l.feature_guest_accounts)                                           AS feature_guest_accounts
           , MAX(l.feature_guest_accounts_permissions)                               AS feature_guest_accounts_permissions   
           , MAX(l.feature_id_loaded)                                                AS feature_id_loaded
           , MAX(l.feature_ldap)                                                     AS feature_ldap
           , MAX(l.feature_ldap_groups)                                              AS feature_ldap_groups
           , MAX(l.feature_lock_teammate_name_display)                               AS feature_lock_teammate_name_display
           , MAX(l.feature_message_export)                                           AS feature_message_export
           , MAX(l.feature_metrics)                                                  AS feature_metrics
           , MAX(l.feature_mfa)                                                      AS feature_mfa
           , MAX(l.feature_mhpns)                                                    AS feature_mhpns
           , MAX(l.feature_office365)                                                AS feature_office365
           , MAX(l.feature_password)                                                 AS feature_password
           , MAX(l.feature_saml)                                                     AS feature_saml
           , {{ dbt_utils.surrogate_key('l.date', 'l.license_id', 'l.customer_id')}} AS id
           , MAX(l.timestamp)                                                        AS timestamp
           , COUNT(DISTINCT l.server_id)                                             AS servers
           , MAX(a.timestamp::DATE)                                                  AS last_telemetry_date
           , COALESCE(CASE WHEN SUM(nullif(dau_total,0)) >= SUM(a.active_users)
                        THEN SUM(nullif(dau_total,0)) ELSE SUM(a.active_users)
                        END, 0)                                                      AS server_dau
           , COALESCE(CASE WHEN SUM(nullif(mau_total,0)) >= SUM(a.active_users_monthly)
                        THEN SUM(nullif(mau_total,0)) ELSE SUM(a.active_users_monthly)
                        END , 0)                                                     AS server_mau
         FROM {{ ref('licenses') }} l
         LEFT JOIN (
                    SELECT 
                        a.user_id
                      , l.date
                      , MAX(CASE WHEN a.timestamp::DATE = l.date 
                                THEN COALESCE(a.active_users_daily, a.active_users) 
                                    ELSE 0 END)                                  AS active_users
                      , MAX(CASE WHEN a.timestamp::DATE = l.date 
                                THEN a.active_users_monthly 
                                    ELSE 0 END)                                  AS active_users_monthly
                      , MAX(a.timestamp::date)                                      AS timestamp
                    FROM {{ ref('licenses') }} l
                         JOIN {{ source('mattermost2', 'activity') }} a
                              ON l.server_id = a.user_id
                              AND a.timestamp::DATE <= l.date
                    {{ dbt_utils.group_by(n=2) }}
         ) a
                   ON l.server_id = a.user_id
                   AND a.date = l.date
         LEFT JOIN {{ ref('server_events_by_date') }} e
                   ON l.date = e.date
                   AND l.server_id = e.server_id
         WHERE l.date <= CURRENT_DATE - INTERVAL '1 day'
         AND l.date <= l.server_expire_date
         {% if is_incremental() %}

         AND l.date >= (SELECT MAX(date) FROM {{this}})

         {% endif %}
         {{ dbt_utils.group_by(n=17) }}
     )
     SELECT *
     FROM license_daily_details
