{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH server_details AS (
    SELECT
        server_id
      , MIN(date) AS                                                                      first_active_date
      , MAX(CASE WHEN in_security OR in_mm2_server THEN date ELSE NULL END) AS            last_active_date
      , MIN(CASE WHEN in_security THEN date ELSE NULL END) AS                             first_telemetry_active_date
      , MAX(CASE WHEN in_security THEN date ELSE NULL END) AS                             last_telemetry_active_date
      , MAX(active_user_count) AS                                                         max_active_user_count
      , MAX(CASE WHEN active_user_count > 0 THEN date ELSE NULL END) AS                   last_active_user_date
      , MAX(CASE
                WHEN license_id1 IS NOT NULL OR license_id2 IS NOT NULL THEN date
                                                                        ELSE NULL END) AS last_active_license_date
      , MIN(CASE WHEN license_id1 IS NOT NULL OR license_id2 IS NOT NULL THEN date
                                                                        ELSE NULL END) AS first_active_license_date
      , MIN(version) AS                                                                   first_server_version
    FROM {{ ref('server_daily_details') }}
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
        , s.edition      AS first_server_edition
      FROM server_details sd
      JOIN {{ ref('server_daily_details') }} s
           ON sd.server_id = s.server_id
           AND sd.first_telemetry_active_date = s.date
      GROUP BY 1, 2
    ),
  server_active_users AS (
    SELECT
        server_id
      , dau_total
      , mobile_dau
      , mau_total
      , first_time_mau
      , reengaged_mau
      , current_mau
      , total_events
      , desktop_events
      , web_app_events
      , mobile_events
      , events_alltime
      , mobile_events_alltime
      , USERS
    FROM {{ ref('server_events_by_date') }}
    WHERE DATE = CURRENT_DATE - INTERVAL '1 DAY'
        {{ dbt_utils.group_by(n=14) }}
    ), 
  server_fact AS (
    SELECT
        server_details.server_id
      , server_daily_details.version
      , server_details.first_server_version
      , server_daily_details.edition        AS server_edition
      , fse.first_server_edition
      , upgrades.version_upgrade_count
      , upgrades.edition_upgrade_count
      , server_daily_details.account_sfid   AS last_account_sfid
      , server_daily_details.license_id1    AS last_license_id1
      , server_daily_details.license_id2    AS last_license_id2
      , licenses.first_paid_license_date
      , licenses.first_trial_license_date
      , server_details.first_active_date
      , server_details.last_active_date
      , server_details.first_telemetry_active_date
      , server_details.last_telemetry_active_date
      , server_details.max_active_user_count
      , server_details.last_active_user_date
      , sau.dau_total
      , sau.mobile_dau
      , sau.mau_total
      , sau.first_time_mau
      , sau.reengaged_mau
      , sau.current_mau
      , sau.total_events
      , sau.desktop_events
      , sau.web_app_events
      , sau.mobile_events
      , sau.events_alltime
      , sau.mobile_events_alltime
      , sau.users
      , server_details.last_active_license_date
      , nps.nps_users
      , nps.nps_score
      , nps.promoters
      , nps.detractors
      , nps.passives
      , nps.avg_score                   AS avg_nps_user_score
    FROM server_details
        JOIN {{ ref('server_daily_details') }}
            ON server_details.server_id = server_daily_details.server_id
            AND server_details.last_active_date = server_daily_details.date
        LEFT JOIN {{ ref('nps_server_daily_score') }} nps
            ON server_details.server_id = nps.server_id
            AND nps.date = DATE_TRUNC('day', CURRENT_DATE - INTERVAL '1 DAY')
        LEFT JOIN server_upgrades upgrades
            ON server_details.server_id = upgrades.server_id
        LEFT JOIN server_active_users sau
            ON server_details.server_id = sau.server_id
        LEFT JOIN first_server_edition fse
            ON server_details.server_id = fse.server_id
        LEFT JOIN licenses l
            ON server_details.server_id = licenses.server_id
        {{ dbt_utils.group_by(n=38) }}
    )
SELECT *
FROM server_fact