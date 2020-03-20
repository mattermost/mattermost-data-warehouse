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
           , l.edition
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
           , l.timestamp
           , {{ dbt_utils.surrogate_key('l.date', 'l.license_id', 'l.customer_id')}} AS id
           , COUNT(DISTINCT l.server_id)                                             AS servers
           , MAX(a.timestamp::DATE)                                                  AS last_telemetry_date
           , COALESCE(SUM(dau_total), 0)                                             AS server_dau
           , COALESCE(SUM(mau_total), 0)                                             AS server_mau
         FROM {{ ref('licenses') }} l
         LEFT JOIN (
                    SELECT 
                        a.user_id
                      , l.date
                      , MAX(a.timestamp::date) as timestamp
                    FROM {{ ref('licenses') }} l
                         JOIN {{ source('mattermost2', 'activity') }} a
                              ON l.server_id = a.user_id
                              AND a.timestamp::date <= l.date
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
         {{ dbt_utils.group_by(n=43) }}
     )
     SELECT *
     FROM license_daily_details