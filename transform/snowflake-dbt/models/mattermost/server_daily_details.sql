{{config({
    "materialized": 'incremental',
    "schema": "mattermost"
  })
}}

WITH server_daily_details AS (
    SELECT
        COALESCE(s1.date, s2.date)                                                  AS date
      , coalesce(s1.server_id, s2.server_id)                                        AS server_id
      , coalesce(s1.ip_address, NULL)                                               AS ip_address
      , coalesce(s1.location, NULL)                                                 AS location
      , coalesce(s1.version, s2.version)                                            AS version
      , coalesce(s2.context_library_version, NULL)                                  AS context_library_version
      , coalesce(s2.edition, NULL)                                                  AS edition
      , coalesce(s1.active_user_count, NULL)                                        AS active_user_count
      , coalesce(s1.user_count, NULL)                                               AS user_count
      , coalesce(s2.system_admins, NULL)                                            AS system_admins
      , coalesce(s1.os_type, s2.operating_system)                                   AS operating_system
      , coalesce(s1.db_type, s2.database_type)                                      AS database_type
      , CASE WHEN s1.server_id IS NOT NULL THEN TRUE ELSE FALSE END                 AS in_security
      , CASE WHEN s2.server_id IS NOT NULL THEN TRUE ELSE FALSE END                 AS in_mm2_server
      , CASE WHEN s1.occurrences > 1 OR s2.occurrences > 1 THEN TRUE ELSE FALSE END AS has_dupes
      , CASE WHEN coalesce(s1.ip_count, NULL) > 1 THEN TRUE ELSE FALSE END          AS has_multi_ips
      , coalesce(s2.timestamp, NULL)                                                AS timestamp
      , coalesce(s1.account_sfid, NULL)                                             AS account_sfid
      , coalesce(s1.license_id1, NULL)                                              AS license_id1
      , coalesce(s1.license_id2, NULL)                                              AS license_id2
    FROM {{ ref('server_security_details') }}                    s1
         FULL OUTER JOIN {{ ref('server_server_details') }} s2
                         ON s1.server_id = s2.server_id
                             AND s1.date = s2.date
    WHERE COALESCE(s1.date, s2.date) < CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND COALESCE(s1.date, s2.date)::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
    )
SELECT *
FROM server_daily_details
