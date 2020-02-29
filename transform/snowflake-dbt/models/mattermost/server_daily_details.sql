{{config({
    "materialized": 'incremental',
    "schema": "mattermost"
  })
}}

WITH servers as (
  SELECT 
      coalesce(s1.server_id, s2.server_id)  AS server_id
    , MIN(COALESCE(s1.date, s2.date))       AS min_date
  FROM {{ ref('server_security_details') }}                    s1
         FULL OUTER JOIN {{ ref('server_server_details') }}    s2
                         ON s1.server_id = s2.server_id
                            AND s1.date = s2.date
  GROUP BY 1
),
dates as (
  SELECT 
      d.date
    , s.server_id
  FROM {{ source('util', 'dates') }} d
  JOIN servers s
       ON d.date >= s.min_date
       AND d.date <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE d.date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
  GROUP BY 1, 2
),
  server_daily_details AS (
    SELECT
        d.date                                                                           AS date
      , d.server_id                                                                      AS server_id
      , coalesce(s1.ip_address, NULL)                                                    AS ip_address
      , coalesce(s1.location, NULL)                                                      AS location
      , coalesce(s1.version, s2.version)                                                 AS version
      , coalesce(s2.context_library_version, NULL)                                       AS context_library_version
      , coalesce(s2.edition, NULL)                                                       AS edition
      , coalesce(s1.active_user_count, NULL)                                             AS active_user_count
      , coalesce(s1.user_count, NULL)                                                    AS user_count
      , coalesce(s2.system_admins, NULL)                                                 AS system_admins
      , coalesce(s1.os_type, s2.operating_system)                                        AS operating_system
      , coalesce(s1.db_type, s2.database_type)                                           AS database_type
      , coalesce(s1.account_sfid, s2.account_sfid)                                       AS account_sfid
      , coalesce(s1.license_id1, s2.license_id1)                                         AS license_id1
      , coalesce(s1.license_id2, s2.license_id2)                                         AS license_id2
      , CASE WHEN s1.server_id IS NOT NULL THEN TRUE ELSE FALSE END                      AS in_security
      , CASE WHEN s2.server_id IS NOT NULL THEN TRUE ELSE FALSE END                      AS in_mm2_server
      , CASE WHEN s1.server_id IS NULL AND s2.server_id IS NULL THEN TRUE ELSE FALSE END AS tracking_disabled
      , CASE WHEN s1.occurrences > 1 OR s2.occurrences > 1 THEN TRUE ELSE FALSE END      AS has_dupes
      , CASE WHEN coalesce(s1.ip_count, NULL) > 1 THEN TRUE ELSE FALSE END               AS has_multi_ips
      , coalesce(s2.timestamp, NULL)                                                     AS timestamp
    FROM dates d
         LEFT JOIN {{ ref('server_security_details') }}    s1
                         ON d.server_id = s1.server_id
                             AND d.date = s1.date
         LEFT JOIN {{ ref('server_server_details') }}      s2
                         ON d.server_id = s2.server_id
                             AND d.date = s2.date
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE d.date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
    )
SELECT *
FROM server_daily_details
