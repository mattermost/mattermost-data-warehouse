{{config({
    "materialized": 'incremental',
    "schema": "blp",
    "unique_key": 'id',
    "tags":'nightly'
  })
}}

WITH license        AS (
    SELECT
        l.customerid
      , l.company
      , l.number
      , l.email
      , l.stripeid
      , l.licenseid
      , to_timestamp(l.issuedat / 1000)::DATE  AS issuedat
      , to_timestamp(l.expiresat / 1000)::DATE AS expiresat
    FROM {{ source('licenses', 'licenses') }} l
    {{ dbt_utils.group_by(n=8) }}
),

     max_date        AS (
         SELECT
             d.date
           , l.license_id
           , l.user_id         AS server_id
           , l.customer_id
           , MAX(l.edition)                           AS edition
           , MAX(to_timestamp(l.issued / 1000)::DATE) AS issued_date
           , MAX(to_timestamp(l._start / 1000)::DATE) AS start_date
           , MAX(to_timestamp(l.expire / 1000)::DATE) AS expire_date
           , MAX(l.users)      AS users
           , MAX(l.timestamp)  AS max_timestamp
           , MIN(l.timestamp)  AS license_activation_date
         FROM {{ source('util', 'dates') }} d
         JOIN {{ source('mattermost2','license') }} l
              ON l.timestamp::date <= d.date
              AND d.date <= CURRENT_DATE
              AND d.date >= to_timestamp(l.issued / 1000)::DATE
         {{ dbt_utils.group_by(n=4) }}
     ),

     date_ranges      AS (
       SELECT
           d.date
         , l.customerid
         , l.company
         , l.number
         , l.email
         , l.stripeid
         , l.licenseid
         , l.issuedat
         , l.expiresat
       FROM {{ source('util', 'dates') }} d
            JOIN license l
                 ON d.date >= l.issuedat
                 AND d.date <= CASE WHEN CURRENT_DATE <= l.expiresat THEN CURRENT_DATE ELSE l.expiresat END
       {{ dbt_utils.group_by(n=9) }}
     ), 

     license_details AS (
         SELECT
             m.date                              AS date
           , m.license_id
           , m.server_id                                                             AS server_id
           , MAX(m.customer_id)                                                      AS customer_id
           , MAX(m.edition)                                                          AS edition
           , MAX(m.issued_date)                                                      AS issued_date
           , MAX(m.start_date)                                                       AS start_date
           , MAX(m.expire_date)                                                      AS expire_date
           , MAX(m.users)                                                            AS users
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
           , MAX(m.max_timestamp)                                                    AS timestamp
           , MIN(m.license_activation_date)                                          AS license_activation_date
         FROM max_date           m
              JOIN {{ source('mattermost2' , 'license') }} l
                   ON l.license_id = m.license_id
                       AND l.user_id = m.server_id
                       AND l.customer_id = m.customer_id
                       AND l.timestamp = m.max_timestamp
         {{ dbt_utils.group_by(n=3) }}
     ),

     license_overview AS (
         SELECT 
            lo.licenseid
          , lo.company
          , lo.stripeid
          , lo.customerid
          , lo.license_email
          , lo.master_account_sfid
          , lo.master_account_name
          , lo.account_sfid
          , lo.account_name
          , lo.contact_sfid
          , lo.contact_email
         FROM {{ ref('license_overview') }} lo
         {{ dbt_utils.group_by(n=11) }}
     ),

     license_details_all AS (
         SELECT
             l.date
           , l.licenseid                                                                           AS license_id
           , ld.server_id
           , l.customerid                                                                          AS customer_id
           , l.company
           , ld.edition
           , COALESCE(l.issuedat, ld.issued_date)                                                  AS issued_date
           , COALESCE(ld.start_date, l.issuedat)                                                   AS start_date
           , COALESCE(l.expiresat, ld.expire_date)                                                 AS expire_date
           , COALESCE(elm.account_sfid, lo.master_account_sfid)                                    AS master_account_sfid
           , COALESCE(elm.name, lo.master_account_name)                                            AS master_account_name
           , COALESCE(elm.account_sfid, lo.account_sfid)                                           AS account_sfid
           , COALESCE(elm.name, lo.account_name)                                                   AS account_name
           , l.email                                                                               AS license_email
           , lo.contact_sfid
           , lo.contact_email
           , l.number
           , l.stripeid
           , ld.users
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
           , ld.timestamp
           , {{ dbt_utils.surrogate_key('l.licenseid', 'l.customerid', 'l.date', 'ld.server_id') }} AS id
           , ld.license_activation_date
         FROM date_ranges    l
              LEFT JOIN license_details ld
                        ON l.licenseid = ld.license_id
                        AND l.date = ld.date
              LEFT JOIN license_overview lo
                        ON l.licenseid = lo.licenseid
              LEFT JOIN (
                         SELECT 
                            l.*
                          , a.name
                         FROM {{ ref('enterprise_license_mapping') }} l
                              LEFT JOIN {{ source('orgm', 'account') }} a
                                        ON l.account_sfid = a.sfid
                        ) elm
                        ON l.licenseid = elm.licenseid
         {% if is_incremental() %}

         WHERE COALESCE(ld.timestamp, CURRENT_DATE - INTERVAL '1 DAY') > (SELECT MAX(timestamp) FROM {{this}})
         OR l.date > (SELECT MAX(DATE) FROM {{this}})

         {% endif %}
         {{ dbt_utils.group_by(n=44) }}
     ),

     licenses_window as (
         SELECT
             ld.date
           , ld.license_id
           , ld.server_id
           , ld.customer_id
           , ld.company
           , MAX(ld.edition) OVER (PARTITION BY ld.license_id) AS edition
           , CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN TRUE 
                  WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN TRUE
                  ELSE FALSE END        AS trial
           , ld.issued_date
           , ld.start_date
           , ld.expire_date
           , CASE WHEN lead(ld.start_date, 1) OVER (PARTITION BY ld.date, ld.server_id, ld.customer_id,
                  CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN TRUE 
                  WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN TRUE
                  ELSE FALSE END ORDER BY ld.start_date, ld.users) <= ld.expire_date
                    THEN lead(ld.start_date, 1) OVER (PARTITION BY ld.date, ld.server_id, ld.customer_id,
                  CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN TRUE 
                  WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN TRUE
                  ELSE FALSE END ORDER BY ld.start_date, ld.users) - INTERVAL '1 DAY'
             ELSE
                 ld.expire_date END     AS server_expire_date
           , ld.master_account_sfid
           , ld.master_account_name
           , ld.account_sfid
           , ld.account_name
           , ld.license_email
           , ld.contact_sfid
           , ld.contact_email
           , ld.number
           , ld.stripeid
           , ld.users
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
           , ld.timestamp
           , ld.license_activation_date
           , ld.id
           , CASE WHEN
                  COUNT(CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN ld.server_id
                          WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN ld.server_id
                          ELSE NULL END) OVER (PARTITION BY ld.date, ld.server_id) >= 1
              AND COUNT(ld.server_id) OVER (PARTITION BY ld.date, ld.server_Id) > 
                  COUNT(CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN ld.server_id
                          WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN ld.server_id
                          ELSE NULL END) OVER (PARTITION BY ld.date, ld.server_id)
              THEN TRUE ELSE FALSE END      AS has_trial_and_non_trial
          , CASE WHEN lead(ld.start_date, 1) OVER (PARTITION BY ld.date, ld.server_id,
                  CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN TRUE 
                  WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN TRUE
                  ELSE FALSE END ORDER BY ld.start_date, ld.users) <= ld.expire_date
                    THEN lead(ld.start_date, 1) OVER (PARTITION BY ld.date, ld.server_id,
                  CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN TRUE 
                  WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN TRUE
                  ELSE FALSE END ORDER BY ld.start_date, ld.users) - INTERVAL '1 DAY'
             ELSE
                 ld.expire_date END     AS server_expire_date_join
         FROM license_details_all ld
          {% if is_incremental() %}

         WHERE COALESCE(ld.timestamp, CURRENT_DATE - INTERVAL '1 DAY') > (SELECT MAX(timestamp) FROM {{this}})
         OR ld.date > (SELECT MAX(DATE) FROM {{this}})

         {% endif %}
     ),
     licenses as (
       SELECT
          lw.date                                     AS date
        , lw.license_id                               AS license_id
        , lw.server_id                                AS server_id
        , lw.customer_id                              AS customer_id
        , MAX(lw.company)                             AS company
        , MAX(lw.edition)                             AS edition
        , MAX(lw.trial)                               AS trial
        , MAX(lw.issued_date)                         AS issued_date
        , MAX(lw.start_date)                          AS start_date
        , MAX(lw.expire_date)                         AS expire_date
        , MAX(lw.server_expire_date)                  AS server_expire_date
        , MAX(lw.master_account_sfid)                 AS master_account_sfid
        , MAX(lw.master_account_name)                 AS master_account_name
        , MAX(lw.account_sfid)                        AS account_sfid
        , MAX(lw.account_name)                        AS account_name
        , MAX(lw.license_email)                       AS license_email
        , MAX(lw.contact_sfid)                        AS contact_sfid
        , MAX(lw.contact_email)                       AS contact_email
        , MAX(lw.number)                              AS number
        , MAX(lw.stripeid)                            AS stripeid
        , MAX(lw.users)                               AS users
        , MAX(lw.feature_cluster)                     AS feature_cluster
        , MAX(lw.feature_compliance)                  AS feature_compliance
        , MAX(lw.feature_custom_brand)                AS feature_custom_brand
        , MAX(lw.feature_custom_permissions_schemes)  AS feature_custom_permissions_schemes
        , MAX(lw.feature_data_retention)              AS feature_data_retention
        , MAX(lw.feature_elastic_search)              AS feature_elastic_search
        , MAX(lw.feature_email_notification_contents) AS feature_email_notification_contents
        , MAX(lw.feature_future)                      AS feature_future
        , MAX(lw.feature_google)                      AS feature_google
        , MAX(lw.feature_guest_accounts)              AS feature_guest_accounts
        , MAX(lw.feature_guest_accounts_permissions)  AS feature_guest_accounts_permissions
        , MAX(lw.feature_id_loaded)                   AS feature_id_loaded
        , MAX(lw.feature_ldap)                        AS feature_ldap
        , MAX(lw.feature_ldap_groups)                 AS feature_ldap_groups
        , MAX(lw.feature_lock_teammate_name_display)  AS feature_lock_teammate_name_display
        , MAX(lw.feature_message_export)              AS feature_message_export
        , MAX(lw.feature_metrics)                     AS feature_metrics
        , MAX(lw.feature_mfa)                         AS feature_mfa
        , MAX(lw.feature_mhpns)                       AS feature_mhpns
        , MAX(lw.feature_office365)                   AS feature_office365
        , MAX(lw.feature_password)                    AS feature_password
        , MAX(lw.feature_saml)                        AS feature_saml
        , MAX(lw.timestamp)                           AS timestamp
        , MAX(lw.id)                                  AS id
        , MAX(lw.has_trial_and_non_trial)             AS has_trial_and_non_trial
        , MAX(lw.server_expire_date_join)             AS server_expire_date_join
        , MAX(lw.license_activation_date)             AS license_activation_date
        FROM licenses_window lw
     )
SELECT *
FROM licenses
