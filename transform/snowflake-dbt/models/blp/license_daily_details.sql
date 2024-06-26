{{config({
    "materialized": 'incremental',
    "schema": "blp",
    "unique_key": 'id',
    "tags":["nightly"]
  })
}}

WITH license_daily_details_all as (
         SELECT 
             l.date
           , l.license_id
           , l.customer_id
           , REPLACE(TRIM(l.company), '  ', ' ')                                 AS company
           , l.trial
           , l.issued_date
           , l.start_date
           , l.server_expire_date                                                AS expire_date
           , COALESCE(lsf.account_sfid , l.master_account_sfid) AS master_account_sfid
           , COALESCE(lsf.account_name , l.master_account_name) AS master_account_name
           , COALESCE(lsf.account_sfid , l.account_sfid) AS account_sfid
           , COALESCE(lsf.account_name , l.account_name) AS account_name
           , COALESCE(lsf.license_email, l.license_email) AS license_email
           , COALESCE(lsf.contact_sfid, l.contact_sfid) AS contact_sfid
           , COALESCE(lsf.license_email, l.contact_email) AS contact_email
           , l.number
           , l.stripeid
           , MAX(l.edition)                                                                 AS edition
           , MAX(l.users)                                                                   AS users
           , MAX(l.feature_cluster)                                                         AS feature_cluster
           , MAX(l.feature_compliance)                                                      AS feature_compliance
           , MAX(l.feature_custom_brand)                                                    AS feature_custom_brand
           , MAX(l.feature_custom_permissions_schemes)                                      AS feature_custom_permissions_schemes
           , MAX(l.feature_data_retention)                                                  AS feature_data_retention
           , MAX(l.feature_elastic_search)                                                  AS feature_elastic_search
           , MAX(l.feature_email_notification_contents)                                     AS feature_email_notification_contents                        
           , MAX(l.feature_future)                                                          AS feature_future
           , MAX(l.feature_google)                                                          AS feature_google
           , MAX(l.feature_guest_accounts)                                                  AS feature_guest_accounts
           , MAX(l.feature_guest_accounts_permissions)                                      AS feature_guest_accounts_permissions   
           , MAX(l.feature_id_loaded)                                                       AS feature_id_loaded
           , MAX(l.feature_ldap)                                                            AS feature_ldap
           , MAX(l.feature_ldap_groups)                                                     AS feature_ldap_groups
           , MAX(l.feature_lock_teammate_name_display)                                      AS feature_lock_teammate_name_display
           , MAX(l.feature_message_export)                                                  AS feature_message_export
           , MAX(l.feature_metrics)                                                         AS feature_metrics
           , MAX(l.feature_mfa)                                                             AS feature_mfa
           , MAX(l.feature_mhpns)                                                           AS feature_mhpns
           , MAX(l.feature_office365)                                                       AS feature_office365
           , MAX(l.feature_password)                                                        AS feature_password
           , MAX(l.feature_saml)                                                            AS feature_saml
           , {{ dbt_utils.surrogate_key(['l.date', 'l.license_id', 'l.customer_id'])}}        AS id
           , MAX(l.timestamp)                                                               AS timestamp
           , COUNT(DISTINCT l.server_id)                                                    AS servers
           , MAX(a.timestamp::DATE)                                                         AS last_telemetry_date
           , MIN(a.min_telemetry_date::DATE)                                                AS first_telemetry_date
           , CASE WHEN SUM(e.dau_total) >= SUM(a.active_users)
                        THEN SUM(e.dau_total) ELSE SUM(a.active_users)
                        END                                                                 AS server_dau
           , CASE WHEN SUM(e.mau_total) >= SUM(a.active_users_monthly)
                        THEN SUM(mau_total) ELSE SUM(a.active_users_monthly)
                        END                                                                 AS server_mau
           , SUM(NULLIF(a.registered_users, 0))                                             AS registered_users
           , SUM(NULLIF(a.registered_inactive_users, 0))                                    AS registered_inactive_users
           , SUM(NULLIF(a.registered_deactivated_users, 0))                                 AS registered_deactivated_users
           , SUM(NULLIF(a.posts, 0))                                                        AS posts
           , SUM(NULLIF(a.direct_message_channels, 0))                                      AS direct_message_channels
           , SUM(NULLIF(a.outgoing_webhooks, 0))                                            AS outgoing_webhooks
           , SUM(NULLIF(a.teams, 0))                                                        AS teams
           , SUM(NULLIF(a.private_channels, 0))                                             AS private_channels
           , SUM(NULLIF(a.private_channels_deleted, 0))                                     AS private_channels_deleted
           , SUM(NULLIF(a.public_channels, 0))                                              AS public_channels
           , SUM(NULLIF(a.public_channels_deleted, 0))                                      AS public_channels_deleted
           , SUM(NULLIF(a.bot_accounts, 0))                                                 AS bot_accounts
           , SUM(NULLIF(a.guest_accounts, 0))                                               AS guest_accounts
           , MAX(regexp_substr(s.version,'^[0-9]{1,2}[.]{1}[0-9]{1,2}[.]{1}[0-9]{1,2}'))    AS server_version
         FROM {{ ref('licenses') }} l
         LEFT JOIN (
                    SELECT 
                        a.user_id
                      , l.date
                      , MAX(CASE WHEN a.timestamp::DATE = l.date 
                                THEN COALESCE(a.active_users_daily, a.active_users) 
                                    ELSE NULL END)                                  AS active_users
                      , MAX(CASE WHEN a.timestamp::DATE = l.date 
                                THEN a.active_users_monthly 
                                    ELSE NULL END)                                  AS active_users_monthly
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.registered_users 
                                    ELSE 0 END)                                  AS registered_users
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.registered_inactive_users 
                                    ELSE 0 END)                                  AS registered_inactive_users
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.registered_deactivated_users 
                                    ELSE 0 END)                                  AS registered_deactivated_users
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.posts 
                                    ELSE 0 END)                                  AS posts
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.direct_message_channels
                                    ELSE 0 END)                                  AS direct_message_channels
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.outgoing_webhooks
                                    ELSE 0 END)                                  AS outgoing_webhooks
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.teams
                                    ELSE 0 END)                                  AS teams
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.private_channels
                                    ELSE 0 END)                                  AS private_channels
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.private_channels
                                    ELSE 0 END)                                  AS private_channels_deleted
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.public_channels
                                    ELSE 0 END)                                  AS public_channels
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.public_channels
                                    ELSE 0 END)                                  AS public_channels_deleted
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.bot_accounts
                                    ELSE 0 END)                                  AS bot_accounts
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.guest_accounts
                                    ELSE 0 END)                                  AS guest_accounts
                      , MAX(CASE WHEN a.timestamp::DATE = l.date
                                THEN a.context_library_version 
                                    ELSE NULL END)                                  AS server_version
                      , MAX(a.timestamp::date)                                   AS timestamp
                      , MIN(a.timestamp::date)                                   AS min_telemetry_date
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
         LEFT JOIN {{ ref('server_daily_details') }} s
                   ON l.date = s.date
                   AND l.server_id = s.server_id
        LEFT JOIN {{ ref('license_server_fact') }} lsf
                    ON l.license_id = lsf.license_id
                    AND COALESCE(l.server_id, 'none') = coalesce(lsf.server_id, 'none')
         WHERE l.date <= CURRENT_DATE - INTERVAL '1 day'
         AND l.date <= l.server_expire_date
         AND l.date >= l.issued_date
         {% if is_incremental() %}

         AND l.date > (SELECT MAX(date) FROM {{this}})

         {% endif %}
         {{ dbt_utils.group_by(n=17) }}
     ),

     license_daily_details AS (
        SELECT
            ld.date
          , ld.license_id
          , ld.customer_id
          , ld.company
          , ld.trial
          , MIN(ld.issued_date) OVER (PARTITION BY ld.date, ld.customer_id, ld.license_id)         AS issued_date
          , MIN(ld.start_date) OVER (PARTITION BY ld.date, ld.customer_id, ld.license_id)          AS start_date
          , MAX(ld.expire_date) OVER (PARTITION BY ld.date, ld.customer_id, ld.license_id)         AS expire_date
          , ld.master_account_sfid
          , ld.master_account_name
          , ld.account_sfid
          , ld.account_name
          , ld.license_email
          , ld.contact_sfid
          , ld.contact_email
          , ld.number
          , ld.stripeid
          , CASE WHEN ld.edition IS NOT NULL THEN ld.edition
                 WHEN MAX(ld.feature_compliance) OVER (PARTITION BY ld.date, ld.customer_id, ld.license_id) 
                 THEN 'E20' ELSE 'E10' END                                                   AS edition
          , ld.users                                                                         AS license_users
          , MAX(ld.users) OVER (PARTITION BY ld.date, ld.customer_id)                        AS customer_users
          , ld.feature_cluster
          , ld.feature_compliance
          , ld.feature_custom_brand
          , ld.feature_custom_permissions_schemes
          , ld.feature_data_retention
          , ld.feature_elastic_search
          , ld.feature_email_notification_contents
          , ld.feature_future
          , ld.feature_google
          , ld.feature_guest_accounts
          , ld.feature_guest_accounts_permissions
          , ld.feature_id_loaded
          , ld.feature_ldap
          , ld.feature_ldap_groups
          , ld.feature_lock_teammate_name_display
          , ld.feature_message_export
          , ld.feature_metrics
          , ld.feature_mfa
          , ld.feature_mhpns
          , ld.feature_office365
          , ld.feature_password
          , ld.feature_saml
          , ld.id
          , ld.timestamp
          , ld.servers
          , ld.server_dau                                                                    AS license_server_dau
          , MAX(ld.server_dau) OVER (PARTITION BY ld.date, ld.customer_id)                   AS customer_server_dau
          , ld.server_mau                                                                    AS license_server_mau
          , MAX(ld.server_mau) OVER (PARTITION BY ld.date, ld.customer_id)                   AS customer_server_mau
          , ld.last_telemetry_date                                                           AS last_license_telemetry_date
          , MAX(ld.last_telemetry_date) OVER (PARTITION BY ld.date, ld.customer_id)          AS last_customer_telemetry_date
          , ld.first_telemetry_date                                                          AS first_license_telemetry_date
          , MIN(ld.first_telemetry_date) OVER (PARTITION BY ld.date, ld.customer_id)         AS first_customer_telemetry_date
          , ld.registered_users                                                              AS license_registered_users
          , MAX(ld.registered_users) OVER (PARTITION BY ld.date, ld.customer_id)             AS customer_registered_users
          , ld.registered_inactive_users                                                     AS license_registered_inactive_users
          , MAX(ld.registered_inactive_users) OVER (PARTITION BY ld.date, ld.customer_id)    AS customer_registered_inactive_users
          , ld.registered_deactivated_users                                                  AS license_registered_deactivated_users
          , MAX(ld.registered_deactivated_users) OVER (PARTITION BY ld.date, ld.customer_id) AS customer_registered_deactivated_users
          , ld.posts                                                                         AS license_posts
          , MAX(ld.posts) OVER (PARTITION BY ld.date, ld.customer_id)                        AS customer_posts
          , ld.direct_message_channels                                                       AS license_direct_message_channels
          , MAX(ld.direct_message_channels) OVER (PARTITION BY ld.date, ld.customer_id)      AS customer_direct_message_channels
          , ld.outgoing_webhooks                                                             AS license_outgoing_webooks
          , MAX(ld.outgoing_webhooks) OVER (PARTITION BY ld.date, ld.customer_id)            AS customer_outgoing_webooks
          , ld.teams                                                                         AS license_teams
          , MAX(ld.teams) OVER (PARTITION BY ld.date, ld.customer_id)                        AS customer_teams
          , ld.private_channels                                                              AS license_private_channels
          , MAX(ld.private_channels) OVER (PARTITION BY ld.date, ld.customer_id)             AS customer_private_channels
          , ld.private_channels_deleted                                                      AS license_private_channels_deleted
          , MAX(ld.private_channels_deleted) OVER (PARTITION BY ld.date, ld.customer_id)     AS customer_private_channels_deleted
          , ld.public_channels                                                               AS license_public_channels
          , MAX(ld.public_channels) OVER (PARTITION BY ld.date, ld.customer_id)              AS customer_public_channels
          , ld.public_channels_deleted                                                       AS license_public_channels_deleted
          , MAX(ld.public_channels_deleted) OVER (PARTITION BY ld.date, ld.customer_id)      AS customer_public_channels_deleted
          , ld.bot_accounts                                                                  AS license_bot_accounts
          , MAX(ld.bot_accounts) OVER (PARTITION BY ld.date, ld.customer_id)                 AS customer_bot_accounts
          , ld.server_version                                                                AS license_server_version
          , MAX(ld.server_version) OVER (PARTITION BY ld.date, ld.customer_id)               AS customer_server_version
          , ROW_NUMBER() OVER 
                             (PARTITION BY ld.date, ld.company, ld.trial 
                              ORDER BY CASE WHEN ld.users IS NOT NULL THEN ld.expire_date 
                              ELSE ld.start_date END desc NULLS LAST, ld.users desc NULLS LAST, ld.last_telemetry_date desc NULLS LAST, ld.edition desc NULLS LAST)   AS customer_rank
          , ld.guest_accounts                                                                         AS license_guest_accounts
          , MAX(ld.guest_accounts) OVER (PARTITION BY ld.date, ld.customer_id)                        AS customer_guest_accounts
        FROM license_daily_details_all ld
     )
     SELECT *
     FROM license_daily_details
