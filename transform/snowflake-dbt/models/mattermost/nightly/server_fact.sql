{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH server_details AS (
    SELECT
        server_id
      , MIN(CASE WHEN in_security OR in_mm2_server THEN date ELSE NULL END) AS            first_active_date
      , MAX(CASE WHEN in_security OR in_mm2_server THEN date ELSE NULL END) AS            last_active_date
      , MIN(CASE WHEN in_security THEN date ELSE NULL END) AS                             first_telemetry_active_date
      , MAX(CASE WHEN in_security THEN date ELSE NULL END) AS                             last_telemetry_active_date
      , MAX(CASE WHEN coalesce(active_users_daily, active_users) > active_user_count 
              THEN coalesce(active_users_daily, active_users)
              ELSE active_user_count END) AS                                              max_active_user_count
      , MAX(CASE WHEN active_user_count > 0 or coalesce(active_users_daily, active_users) > 0 THEN date ELSE NULL END) AS                   last_active_user_date
      , MAX(CASE
                WHEN license_id1 IS NOT NULL OR license_id2 IS NOT NULL THEN date
                                                                        ELSE NULL END) AS last_active_license_date
      , MIN(CASE WHEN license_id1 IS NOT NULL OR license_id2 IS NOT NULL THEN date
                                                                        ELSE NULL END) AS first_active_license_date
      , MIN(CASE WHEN version IS NOT NULL THEN date ELSE NULL END) AS                     first_server_version_date
      , MIN(CASE WHEN edition IS NOT NULL THEN date ELSE NULL END)                     AS first_edition_date
      , MAX(CASE WHEN edition IS NOT NULL THEN date ELSE NULL END)                     AS last_edition_date
      , MIN(CASE WHEN in_mm2_server THEN date ELSE NULL END)                           AS first_mm2_telemetry_date
      , MAX(CASE WHEN in_mm2_server THEN date ELSE NULL END)                           AS last_mm2_telemetry_date
    FROM {{ ref('server_daily_details_ext') }}
    GROUP BY 1
    ), 
    licenses AS (
      SELECT 
          server_id
        , MIN(CASE WHEN NOT TRIAL THEN date ELSE NULL END) AS first_paid_license_date
        , MIN(CASE WHEN TRIAL THEN date ELSE NULL END)     AS first_trial_license_date
      FROM {{ ref('licenses') }}
      GROUP BY 1
    ),
  server_upgrades AS (
    SELECT
        server_id
      , COUNT(
        CASE WHEN CURRENT_VERSION
      > PREV_VERSION THEN server_id ELSE NULL END) AS version_upgrade_count
      , COUNT(
        CASE WHEN PREV_EDITION = 'false' AND CURRENT_EDITION = 'true' THEN server_id ELSE NULL END) AS edition_upgrade_count
    FROM {{ ref('server_upgrades') }}
    GROUP BY 1
    ),
    first_server_edition AS (
      SELECT
          s.server_id
        , MAX(CASE WHEN sd.first_server_version_date = s.date THEN s.version ELSE NULL END)      AS first_server_version
        , MAX(CASE WHEN sd.first_edition_date = s.date THEN s.edition ELSE NULL END)             AS first_server_edition
        , MAX(CASE WHEN sd.last_edition_date = s.date THEN s.edition ELSE NULL END)              AS edition
        , MAX(sd.first_edition_date)                                                             AS first_edition_date
        , MAX(sd.last_edition_date)                                                              AS last_edition_date
      FROM server_details sd
      JOIN {{ ref('server_daily_details') }} s
           ON sd.server_id = s.server_id
           AND (sd.first_edition_date = s.date
           OR sd.last_edition_date = s.date
           OR sd.first_server_version_date = s.date)
      GROUP BY 1
    ),
  last_server_date AS (
    SELECT
        server_id
      , MAX(CASE WHEN total_events > 0 THEN date ELSE NULL END) AS last_event_date
    FROM {{ ref('server_events_by_date') }}
    GROUP BY 1
  ),
  server_active_users AS (
    SELECT
        s1.server_id
      , s1.dau_total
      , s1.mobile_dau
      , s1.mau_total
      , s1.first_time_mau
      , s1.reengaged_mau
      , s1.current_mau
      , s1.total_events
      , s1.desktop_events
      , s1.web_app_events
      , s1.mobile_events
      , s1.events_alltime
      , s1.mobile_events_alltime
      , s1.USERS
      , s2.last_event_date
    FROM {{ ref('server_events_by_date') }} s1
    JOIN last_server_date s2
         ON s1.server_id = s2.server_id
         AND s1.date = s2.last_event_date
        {{ dbt_utils.group_by(n=15) }}
    ), 
  server_fact AS (
    SELECT
        server_details.server_id
      , MAX(server_daily_details.version)                 AS version
      , MAX(fse.first_server_version)          AS first_server_version
      , MAX(fse.edition)                                  AS server_edition
      , MAX(fse.first_server_edition)                     AS first_server_edition
      , MAX(server_details.first_telemetry_active_date)   AS first_telemetry_active_date
      , MAX(server_details.last_telemetry_active_date)    AS last_telemetry_active_date
      , MAX(server_details.first_mm2_telemetry_date)      AS first_mm2_telemetry_date
      , MAX(server_details.last_mm2_telemetry_date)       AS last_mm2_telemetry_date
      , MAX(upgrades.version_upgrade_count)               AS version_upgrade_count
      , MAX(upgrades.edition_upgrade_count)               AS edition_upgrade_count
      , MAX(CASE WHEN oauth.enable_gitlab_oauth THEN true
              ELSE FALSE END)                             AS gitlab_install
      , MAX(server_daily_details.account_sfid)            AS last_account_sfid
      , MAX(server_daily_details.license_id1)             AS last_license_id1
      , MAX(server_daily_details.license_id2)             AS last_license_id2
      , MAX(licenses.first_paid_license_date)             AS first_paid_license_date
      , MAX(licenses.first_trial_license_date)            AS first_trial_license_date
      , MAX(server_details.first_active_date)             AS first_active_date
      , MAX(server_details.last_active_date)              AS last_active_date
      , MAX(server_details.max_active_user_count)         AS max_active_user_count
      , MAX(server_details.last_active_user_date)         AS last_telemetry_active_user_date
      , MAX(sau.last_event_date)                          AS last_event_active_user_date
      , MAX(sau.dau_total)                                AS dau_total
      , MAX(sau.mobile_dau)                               AS mobile_dau
      , MAX(sau.mau_total)                                AS mau_total
      , MAX(sau.first_time_mau)                           AS first_time_mau
      , MAX(sau.reengaged_mau)                            AS reengaged_mau
      , MAX(sau.current_mau)                              AS current_mau
      , MAX(sau.total_events)                             AS total_events
      , MAX(sau.desktop_events)                           AS desktop_events
      , MAX(sau.web_app_events)                           AS web_app_events
      , MAX(sau.mobile_events)                            AS mobile_events
      , MAX(sau.events_alltime)                           AS events_alltime
      , MAX(sau.mobile_events_alltime)                    AS mobile_events_alltime
      , MAX(sau.users)                                    AS users
      , MAX(server_details.last_active_license_date)      AS last_active_license_date
      , MAX(nps.nps_users)                                AS nps_users
      , MAX(nps.nps_score)                                AS nps_score
      , MAX(nps.promoters)                                AS promoters
      , MAX(nps.detractors)                               AS detractors
      , MAX(nps.passives)                                 AS passives
      , MAX(nps.avg_score)                                AS avg_nps_user_score
    FROM server_details
        JOIN {{ ref('server_daily_details') }}
            ON server_details.server_id = server_daily_details.server_id
            AND (server_details.last_active_date = server_daily_details.date
            OR server_details.last_active_date - INTERVAL '1 DAY' = server_daily_details.date)
        LEFT JOIN {{ ref('nps_server_daily_score') }} nps
            ON server_details.server_id = nps.server_id
            AND nps.date = DATE_TRUNC('day', CURRENT_DATE - INTERVAL '1 DAY')
        LEFT JOIN server_upgrades upgrades
            ON server_details.server_id = upgrades.server_id
        LEFT JOIN server_active_users sau
            ON server_details.server_id = sau.server_id
        LEFT JOIN first_server_edition fse
            ON server_details.server_id = fse.server_id
        LEFT JOIN licenses
            ON server_details.server_id = licenses.server_id
        LEFT JOIN {{ ref('server_oauth_details') }} oauth
            ON server_details.server_id = oauth.server_id
            AND server_details.first_mm2_telemetry_date = oauth.date
        {{ dbt_utils.group_by(n=1) }}
    )
SELECT *
FROM server_fact