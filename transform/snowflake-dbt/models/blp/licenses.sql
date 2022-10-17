{{config({
    "materialized": 'incremental',
    "schema": "blp",
    "unique_key": 'id',
    "tags":'nightly'
  })
}}

WITH license_new AS (
      SELECT
        COALESCE(c.metadata:"id"::VARCHAR, c.metadata:"cws-customer")                                    AS customerid
      , COALESCE(c.name, c.metadata:"company"::VARCHAR, SPLIT_PART(c.email, '@', 2)::VARCHAR)            AS company
      , MAX(COALESCE(c.metadata:"number"::INT, c.metadata:"netsuite_customer_id"::INT))                  AS number
      , c.email
      , c.id                                                                                             AS stripe_id
      , COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL)                                             AS licenseid
      , (s.start_date)::DATE                                                                   AS issuedat
      , (s.current_period_end)::DATE                                                                     AS expiresat
      , FALSE                                                                                            AS blapi
      , COALESCE(s.quantity, c.metadata:"seats"::INT)                                                    AS users
      , COALESCE(c.metadata:"sku"::VARCHAR, SPLIT_PART(s.plan:"name"::VARCHAR, ' ', 2)) AS edition
    FROM {{ source('stripe_raw', 'subscriptions')}} s
    JOIN {{ source('stripe_raw', 'customers') }} c
      ON s.customer = c.id
    WHERE COALESCE(c.metadata:"id"::VARCHAR, c.metadata:"cws-customer") NOT IN (SELECT customerid FROM {{ source('licenses', 'licenses') }} GROUP BY 1)
    AND COALESCE(c.metadata:"id"::VARCHAR, c.metadata:"cws-customer") IS NOT NULL
    AND (COALESCE(s.metadata:"sku"::VARCHAR, SPLIT_PART(s.plan:"name"::VARCHAR, ' ', 2)) IN
      ('E20', 'E10') OR 
      (s.plan:"name"::VARCHAR IN ('Self-Hosted Enterprise','Self-Hosted Professional')))
    AND s.status NOT IN ('incomplete_expired')
    GROUP BY 1, 2, 4, 5, 6, 7, 8, 9, 10, 11
),

license_old        AS (
    SELECT
        l.customerid
      , l.company
      , l.number
      , l.email
      , l.stripeid
      , l.licenseid
      , to_timestamp(l.issuedat / 1000)::DATE  AS issuedat
      , to_timestamp(l.expiresat / 1000)::DATE AS expiresat
      , FALSE AS blapi
      , NULL as users
      , NULL as edition
    FROM {{ source('licenses', 'licenses') }} l
    {{ dbt_utils.group_by(n=11) }}

    UNION ALL

    SELECT
        l.server_id as customerid
      , CASE WHEN pd.domain_name is null then split_part(l.email, '@', 2) else l.name end as company
      , MAX(uniform(10000, 99999, random())::int) as number
      , l.email
      , COALESCE(s.id, null) as stripeid
      , l.id as licenseid
      , license_issued_at::date as issuedat
      , end_date::date as expiresat
      , TRUE AS blapi
      , users
      , 'E20 Trial' AS edition
    FROM {{ source('blapi', 'trial_requests')}} l
    LEFT JOIN {{ source('stripe_raw', 'subscriptions') }} s
          ON l.id = s.metadata:"cws-license-id"
    LEFT JOIN {{ source('util', 'public_domains')}} pd
          ON split_part(l.email, '@', 2) = pd.domain_name
    WHERE l.site_url != 'http://localhost:8065'
    GROUP BY 1, 2, 4, 5, 6, 7, 8, 9, 10, 11),

    license AS (

    SELECT 
        *
    FROM license_old
    WHERE NOT EXISTS (select licenseid FROM license_new new
                    WHERE license_old.licenseid = new.licenseid GROUP BY 1) 

    UNION all

    SELECT
        *
    FROM license_new
),

     max_segment_date        AS (
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
         FROM {{ ref('dates') }} d
         JOIN {{ source('mattermost2','license') }} l
              ON l.timestamp::date <= d.date::date
              AND d.date::date <= CURRENT_DATE::date
              AND d.date::date >= to_timestamp(l.issued / 1000)::DATE
         {{ dbt_utils.group_by(n=4) }}
     ),

          max_rudder_date        AS (
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
         FROM {{ ref('dates') }} d
         JOIN {{ source('mm_telemetry_prod','license') }} l
              ON l.timestamp::date <= d.date::date
              AND d.date::date <= CURRENT_DATE::date
              AND d.date::date >= to_timestamp(l.issued / 1000)::DATE
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
         , CASE WHEN l.blapi THEN l.customerid ELSE NULL END AS server_id
         , l.users
         , l.edition
         FROM {{ ref('dates') }} d
            JOIN license l
                 ON d.date::date >= l.issuedat::date
                 AND d.date::date <= IFF(CURRENT_DATE::date <= l.expiresat::date, CURRENT_DATE::date, l.expiresat::date)
       {{ dbt_utils.group_by(n=12) }}
     ), 

     license_details AS (
         SELECT
             COALESCE(r.timestamp::date, s.timestamp::date)                            AS date
           , COALESCE(r.license_id, s.license_id)                                    AS license_id
           , COALESCE(r.user_id, s.user_id)                                                             AS server_id
           , MAX(COALESCE(r.customer_id, s.customer_id))                                                      AS customer_id
           , MAX(COALESCE(r.edition, s.edition))                                                          AS edition
           , MAX(COALESCE(r.issued_date, s.issued_date))                                                      AS issued_date
           , MAX(COALESCE(r.start_date, s.start_date))                                                       AS start_date
           , MAX(COALESCE(r.expire_date, s.expire_date))                                                      AS expire_date
           , MAX(COALESCE(r.users, s.users))                                                            AS users
           , MAX(COALESCE(r.feature_cluster, s.feature_cluster))                                                  AS feature_cluster
           , MAX(COALESCE(r.feature_compliance, s.feature_compliance))                                               AS feature_compliance
           , MAX(COALESCE(s.feature_custom_brand, NULL ))                                             AS feature_custom_brand
           , MAX(COALESCE(r.feature_custom_permissions_schemes, s.feature_custom_permissions_schemes))                               AS feature_custom_permissions_schemes
           , MAX(COALESCE(r.feature_data_retention, s.feature_data_retention))                                           AS feature_data_retention
           , MAX(COALESCE(r.feature_elastic_search, s.feature_elastic_search))                                           AS feature_elastic_search
           , MAX(COALESCE(r.feature_email_notification_contents, s.feature_email_notification_contents))                              AS feature_email_notification_contents                        
           , MAX(COALESCE(r.feature_future, s.feature_future))                                                   AS feature_future
           , MAX(COALESCE(r.feature_google, s.feature_google))                                                   AS feature_google
           , MAX(COALESCE(r.feature_guest_accounts, s.feature_guest_accounts))                                           AS feature_guest_accounts
           , MAX(COALESCE(r.feature_guest_accounts_permissions, s.feature_guest_accounts_permissions))                               AS feature_guest_accounts_permissions   
           , MAX(COALESCE(r.feature_id_loaded, s.feature_id_loaded))                                                AS feature_id_loaded
           , MAX(COALESCE(r.feature_ldap, s.feature_ldap))                                                     AS feature_ldap
           , MAX(COALESCE(r.feature_ldap_groups, s.feature_ldap_groups))                                              AS feature_ldap_groups
           , MAX(COALESCE(r.feature_lock_teammate_name_display, s.feature_lock_teammate_name_display))                               AS feature_lock_teammate_name_display
           , MAX(COALESCE(r.feature_message_export, s.feature_message_export))                                           AS feature_message_export
           , MAX(COALESCE(r.feature_metrics, s.feature_metrics))                                                  AS feature_metrics
           , MAX(COALESCE(r.feature_mfa, s.feature_mfa))                                                      AS feature_mfa
           , MAX(COALESCE(r.feature_mhpns, s.feature_mhpns))                                                    AS feature_mhpns
           , MAX(COALESCE(r.feature_office365, s.feature_office365))                                                AS feature_office365
           , MAX(COALESCE(s.feature_password, NULL))                                                 AS feature_password
           , MAX(COALESCE(r.feature_saml, s.feature_saml))                                                     AS feature_saml
           , MAX(COALESCE(r.max_timestamp, s.max_timestamp))                                                    AS timestamp
           , MIN(COALESCE(r.license_activation_date, s.license_activation_date))                                          AS license_activation_date
           , MAX(COALESCE(r.FEATURE_ADVANCED_LOGGING, NULL)) AS feature_advanced_logging
           , MAX(COALESCE(r.FEATURE_CLOUD, NULL)) AS feature_cloud
           , MAX(COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)) AS installation_id
         FROM (
              SELECT l.*, m.issued_date, m.start_date, m.expire_date, m.license_activation_date, m.max_timestamp
              FROM max_segment_date           m
              JOIN {{ source('mattermost2' , 'license') }} l
                   ON l.license_id = m.license_id
                       AND l.user_id = m.server_id
                       AND l.customer_id = m.customer_id
                       AND l.timestamp = m.max_timestamp
              ) s
          FULL OUTER JOIN
              (
                SELECT l.*, m.issued_date, m.start_date, m.expire_date, m.license_activation_date, m.max_timestamp
                FROM max_rudder_date  m
                JOIN {{ source('mm_telemetry_prod', 'license') }} l
                  ON  l.license_id = m.license_id
                       AND l.user_id = m.server_id
                       AND l.customer_id = m.customer_id
                       AND l.timestamp = m.max_timestamp
              ) r
          ON s.license_id = r.license_id
          AND s.user_id = r.user_id
          AND s.timestamp::date = r.timestamp::date
          AND s.customer_id = r.customer_id
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
           , COALESCE(ld.server_id, l.server_id)                                                   AS server_id
           , l.customerid                                                                          AS customer_id
           , l.company
           , COALESCE(l.edition, ld.edition)                                                       AS edition
           , COALESCE(l.issuedat, ld.issued_date)                                                  AS issued_date
           , COALESCE(l.issuedat, ld.start_date)                                                   AS start_date
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
           , COALESCE(l.users, ld.users)                                                           AS users
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
           , {{ dbt_utils.surrogate_key(['l.licenseid', 'l.customerid', 'l.date', 'COALESCE(ld.server_id, l.server_id)']) }} AS id
           , ld.license_activation_date
           , ld.installation_id
           , ld.feature_advanced_logging
           , ld.feature_cloud
         FROM date_ranges    l
              LEFT JOIN license_details ld
                        ON l.licenseid = ld.license_id
                        AND l.date::date = ld.date::date
              LEFT JOIN license_overview lo
                        ON COALESCE(l.licenseid, l.customerid) = COALESCE(lo.licenseid, lo.customerid)
              LEFT JOIN (
                         SELECT 
                            l.*
                          , a.name
                         FROM {{ ref('enterprise_license_mapping') }} l
                              LEFT JOIN {{ ref( 'account') }} a
                                        ON l.account_sfid = a.sfid
                        ) elm
                        ON l.licenseid = elm.licenseid
         {% if is_incremental() %}

         WHERE l.date >= (SELECT MAX(DATE) FROM {{this}})

         {% endif %}
         {{ dbt_utils.group_by(n=47) }}
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
                  WHEN ld.edition = 'E20 Trial' THEN TRUE
                  ELSE FALSE END        AS trial
           , ld.issued_date
           , ld.start_date
           , ld.expire_date
           , CASE WHEN lead(ld.start_date, 1) OVER (PARTITION BY ld.date, ld.server_id, ld.customer_id,
                  CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN TRUE 
                  WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN TRUE
                  WHEN ld.edition = 'E20 Trial' THEN TRUE
                  ELSE FALSE END ORDER BY ld.start_date, ld.users) <= ld.expire_date
                    THEN lead(ld.start_date, 1) OVER (PARTITION BY ld.date, ld.server_id, ld.customer_id,
                  CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN TRUE 
                  WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN TRUE
                  WHEN ld.edition = 'E20 Trial' THEN TRUE
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
                          WHEN ld.edition = 'E20 Trial' THEN ld.server_id
                          ELSE NULL END) OVER (PARTITION BY ld.date, ld.server_id) >= 1
              AND COUNT(ld.server_id) OVER (PARTITION BY ld.date, ld.server_Id) > 
                  COUNT(CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN ld.server_id
                          WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN ld.server_id
                          WHEN ld.edition = 'E20 Trial' THEN ld.server_id
                          ELSE NULL END) OVER (PARTITION BY ld.date, ld.server_id)
              THEN TRUE ELSE FALSE END      AS has_trial_and_non_trial
          , CASE WHEN lead(ld.start_date, 1) OVER (PARTITION BY ld.date, ld.server_id,
                  CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN TRUE 
                          WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN TRUE
                          WHEN ld.edition = 'E20 Trial' THEN TRUE
                          ELSE FALSE END ORDER BY ld.start_date, ld.users) <= ld.expire_date
                    THEN lead(ld.start_date, 1) OVER (PARTITION BY ld.date, ld.server_id,
                  CASE WHEN REGEXP_SUBSTR(company, '[^a-zA-Z]TRIAL$') in ('-TRIAL', 'TRIAL', ' TRIAL') THEN TRUE 
                  WHEN DATEDIFF(day, ld.start_date, ld.expire_date) <= 35 THEN TRUE
                          WHEN ld.edition = 'E20 Trial' THEN TRUE
                  ELSE FALSE END ORDER BY ld.start_date, ld.users) - INTERVAL '1 DAY'
             ELSE
                 ld.expire_date END     AS server_expire_date_join
            , ld.installation_id
            , ld.feature_advanced_logging
            , ld.feature_cloud
         FROM license_details_all ld
          {% if is_incremental() %}

         WHERE ld.date >= (SELECT MAX(DATE) FROM {{this}})

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
        , MAX(lw.installation_id)                     AS installation_id
        , MAX(lw.feature_advanced_logging)            AS feature_advanced_logging
        , MAX(lw.feature_cloud)                       AS feature_cloud
        FROM licenses_window lw
        {{ dbt_utils.group_by(n=4) }}
     )
SELECT *
FROM licenses
