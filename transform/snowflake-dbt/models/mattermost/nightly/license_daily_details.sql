{{config({
    "materialized": 'incremental',
    "schema": "mattermost",
    "unique_key": 'id'
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
                      AND d.date <= CASE WHEN CURRENT_DATE - interval '1 day' <= m.expire_date THEN CURRENT_DATE - interval '1 day' ELSE m.expire_date END
         {% if is_incremental() %}

         WHERE d.date >= (SELECT MAX(date) FROM {{this}})

         {% endif %}
        {{ dbt_utils.group_by(n=4) }}
     ),

     license_daily_details as (
         SELECT 
             d.date
           , l.license_id
           , l.server_id
           , l.customer_id
           , l.company
           , l.edition
           , l.trial
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
           , l.timestamp
           , {{ dbt_utils.surrogate_key('d.date', 'l.license_id', 'l.server_id')}} AS id
           , MAX(a.timestamp::DATE) as last_telemetry_date
           , COUNT(DISTINCT CASE WHEN e.active THEN e.user_id ELSE NULL END)       AS license_dau
           , COUNT(DISTINCT CASE WHEN e.mau THEN e.user_id ELSE NULL END)          AS license_mau
         FROM dates d
         JOIN {{ ref('licenses') }} l
              ON d.license_id = l.license_id
         LEFT JOIN (
                    SELECT 
                        timestamp::date as timestamp
                      , user_id
                    FROM {{ source('mattermost2', 'activity') }} 
                    {{ dbt_utils.group_by(n=2) }}
         ) a
                   ON d.server_id = a.user_id
                   AND a.timestamp::DATE <= d.date
         WHERE d.date <= CURRENT_DATE - INTERVAL '1 day'
         {% if is_incremental() %}

         AND d.date >= (SELECT MAX(date) FROM {{this}})

         {% endif %}
         {{ dbt_utils.group_by(n=44) }}
     )
     SELECT *
     FROM license_daily_details