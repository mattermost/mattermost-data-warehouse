{{config({
    "materialized": 'incremental',
    "schema": "mattermost",
    "unique_key":'id'
  })
}}

WITH servers as (
  SELECT 
      coalesce(s2.server_id, s1.server_id)                 AS server_id
    , CASE WHEN MIN(COALESCE(s1.date, s2.date)) <= MIN(COALESCE(s2.date, s1.date)) 
            THEN MIN(COALESCE(s1.date, s2.date)) 
              ELSE MIN(COALESCE(s2.date, s1.date)) END     AS min_date
    , CASE WHEN MAX(CURRENT_DATE) <= 
                  CASE WHEN MAX(COALESCE(s1.date, s2.date)) >= MAX(COALESCE(s2.date, s1.date)) 
                    THEN MAX(COALESCE(s1.date, s2.date)) 
                    ELSE MAX(COALESCE(s2.date, s1.date)) END
          THEN MAX(CURRENT_DATE)
          ELSE CASE WHEN MAX(COALESCE(s1.date, s2.date)) >= MAX(COALESCE(s2.date, s1.date)) 
                    THEN MAX(COALESCE(s1.date, s2.date)) 
                    ELSE MAX(COALESCE(s2.date, s1.date)) END
          END                                             AS max_date
  FROM {{ ref('server_security_details') }}                    s1
         FULL OUTER JOIN {{ ref('server_server_details') }}    s2
                         ON s1.server_id = s2.server_id
                            AND s1.date = s2.date
  WHERE COALESCE(s1.date, s2.date) <= CURRENT_DATE
  GROUP BY 1
),
dates as (
  SELECT 
      d.date
    , s.server_id
  FROM {{ source('util', 'dates') }} d
  JOIN servers s
       ON d.date >= s.min_date
       AND d.date <= s.max_date
  GROUP BY 1, 2
),
  server_daily_details_window AS (
    SELECT
        d.date                                                                           AS date
      , d.server_id                                                                      AS server_id
      , CASE WHEN coalesce(s2.context_ip, s1.ip_address) IS NULL THEN
          MAX(coalesce(s2.context_ip, s1.ip_address)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.context_ip, s1.ip_address) END                                AS ip_address
      , CASE WHEN coalesce(s1.location, NULL) IS NULL THEN
          MAX(coalesce(s1.location, NULL)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s1.location, NULL) END                                           AS location
      , CASE WHEN coalesce(s2.version, s1.version) IS NULL THEN 
          MAX(coalesce(s2.version, s1.version)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.version, s1.version) END                                      AS version
      , coalesce(s2.context_library_version, NULL)                                       AS context_library_version
      , CASE WHEN coalesce(s2.edition, NULL) IS NULL THEN 
          MAX(coalesce(s2.edition, NULL)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.edition, NULL) END                                            AS edition
      , coalesce(s1.active_user_count, NULL)                                             AS active_user_count
      , coalesce(s1.user_count, NULL)                                                    AS user_count
      , coalesce(s2.system_admins, NULL)                                                 AS system_admins
      , CASE WHEN coalesce(s2.operating_system, s1.os_type) IS NULL THEN 
          MAX(coalesce(s2.operating_system, s1.os_type)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.operating_system, s1.os_type) END                             AS operating_system
      , CASE WHEN coalesce(s2.database_type, s1.db_type) IS NULL THEN 
          MAX(coalesce(s2.database_type, s1.db_type)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.database_type, s1.db_type) END                                AS database_type
      , CASE WHEN coalesce(s2.master_account_sfid, s1.master_account_sfid) IS NULL THEN 
          MAX(coalesce(s2.master_account_sfid, s1.master_account_sfid)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.master_account_sfid, s1.master_account_sfid) END              AS master_account_sfid
      , CASE WHEN coalesce(s2.account_sfid, s1.account_sfid) IS NULL THEN 
          MAX(coalesce(s2.master_account_sfid, s1.master_account_sfid)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.master_account_sfid, s1.master_account_sfid) END              AS account_sfid
      , CASE WHEN coalesce(s2.license_id1, s1.license_id1) IS NULL THEN 
          MAX(coalesce(s2.master_account_sfid, s1.master_account_sfid)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.master_account_sfid, s1.master_account_sfid) END              AS license_id1
      , CASE WHEN coalesce(s2.license_id2, s1.license_id2) IS NULL THEN 
          MAX(coalesce(s2.master_account_sfid, s1.master_account_sfid)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.master_account_sfid, s1.master_account_sfid) END              AS license_id2
      , CASE WHEN coalesce(s2.license_email, s1.license_email) IS NULL THEN 
          MAX(coalesce(s2.master_account_sfid, s1.master_account_sfid)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.master_account_sfid, s1.master_account_sfid) END              AS license_email
      , CASE WHEN coalesce(s2.license_contact_sfid, s1.license_contact_sfid) IS NULL THEN 
          MAX(coalesce(s2.master_account_sfid, s1.master_account_sfid)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE coalesce(s2.master_account_sfid, s1.master_account_sfid) END              AS license_contact_sfid
      , CASE WHEN s1.server_id IS NOT NULL THEN TRUE ELSE FALSE END                      AS in_security
      , CASE WHEN s2.server_id IS NOT NULL THEN TRUE ELSE FALSE END                      AS in_mm2_server
      , CASE WHEN s1.server_id IS NULL AND s2.server_id IS NULL THEN TRUE ELSE FALSE END AS tracking_disabled
      , CASE WHEN s1.occurrences > 1 OR s2.occurrences > 1 THEN TRUE ELSE FALSE END      AS has_dupes
      , CASE WHEN coalesce(s1.ip_count, NULL) > 1 THEN TRUE ELSE FALSE END               AS has_multi_ips
      , coalesce(s2.timestamp, NULL)                                                     AS timestamp
      , {{ dbt_utils.surrogate_key('d.date', 'd.server_id') }}                           AS id
      , CASE WHEN COALESCE(s2.database_version, NULL) IS NULL THEN
          MAX(COALESCE(s2.database_version, NULL)) OVER (PARTITION BY d.server_id ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          ELSE COALESCE(s2.database_version, NULL) END                                   AS database_version
    FROM dates d
         LEFT JOIN {{ ref('server_security_details') }}    s1
                         ON d.server_id = s1.server_id
                             AND d.date = s1.date
         LEFT JOIN {{ ref('server_server_details') }}      s2
                         ON d.server_id = s2.server_id
                             AND d.date = s2.date
  ),
  server_daily_details AS (
    SELECT *
    FROM server_daily_details_window
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
     , 22, 23, 24, 25, 26
    )
SELECT *
FROM server_daily_details