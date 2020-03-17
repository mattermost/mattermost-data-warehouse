{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp              AS (
    SELECT
        server_id
      , license_id
      , customer_id
      , MAX(expire_date)  AS expire_date
      , MIN(start_date)   AS start_date
    FROM {{ ref('licenses') }}
    GROUP BY 1, 2, 3
),
     dates as (
         SELECT 
             d.date
           , m.server_id
           , m.license_id
           , m.customer_id
         FROM {{ source('util', 'dates') }}   d
              JOIN max_timestamp m
                   ON d.date >= m.start_date
                      AND d.date <= CASE WHEN (CURRENT_DATE - INTERVAL '1 day') <= m.expire_date THEN CURRENT_DATE - INTERVAL '1 day' ELSE m.expire_date END
          GROUP BY 1, 2, 3, 4
     ),
     server_license_details AS (
         SELECT
             d.date
           , d.server_id
           , d.license_id
           , MAX(start_date)                          AS start_date
           , MAX(edition)                             AS edition
           , MAX(expire_date)                         AS expire_date
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
         FROM dates d
              JOIN {{ ref('licenses') }} l
                   ON d.server_id = l.server_id
                      AND d.license_id = l.license_id
                      AND d.customer_id = l.customer_id
         WHERE d.date <= CURRENT_DATE - INTERVAL '1 day'
         {% if is_incremental() %}
         
         AND d.date > (SELECT MAX(date) FROM {{this}})

         {% endif %}
         GROUP BY 1, 2, 3)
SELECT *
FROM server_license_details