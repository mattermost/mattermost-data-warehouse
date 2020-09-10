{{config({
    'materialized': 'table',
    'schema': 'mattermost'
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
      , MAX(active_users_monthly) AS                                                      max_monthly_active_users
      , MAX(CASE WHEN COALESCE(registered_users,0) > COALESCE(user_count, 0)
            THEN COALESCE(registered_users,0) 
            ELSE COALESCE(user_count,0) END)                                          AS  max_registered_users
      , MAX(coalesce(registered_deactivated_users, 0))                                AS  max_registered_deactivated_users
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
      , MIN(CASE WHEN USER_COUNT > 100 THEN DATE ELSE NULL END)                        AS first_100reg_users_date
      , MIN(CASE WHEN USER_COUNT > 500 THEN DATE ELSE NULL END)                        AS first_500reg_users_date
      , MIN(CASE WHEN USER_COUNT > 1000 THEN DATE ELSE NULL END)                       AS first_1kreg_users_date
      , MIN(CASE WHEN USER_COUNT > 2500 THEN DATE ELSE NULL END)                       AS first_2500reg_users_date
      , MIN(CASE WHEN USER_COUNT > 5000 THEN DATE ELSE NULL END)                       AS first_5kreg_users_date
      , MIN(CASE WHEN USER_COUNT > 10000 THEN DATE ELSE NULL END)                       AS first_10kreg_users_date
      , MAX(POSTS)                                                                     AS max_posts
    FROM {{ ref('server_daily_details_ext') }}
    WHERE DATE <= CURRENT_DATE - INTERVAL '1 DAY'
    GROUP BY 1
    ), 
    licenses AS (
      SELECT 
          server_id
        , MIN(CASE WHEN NOT TRIAL THEN issued_date ELSE NULL END) AS first_paid_license_date
        , MIN(CASE WHEN TRIAL THEN issued_date ELSE NULL END)     AS first_trial_license_date
        , MAX(CASE WHEN NOT TRIAL THEN issued_date ELSE NULL END) AS last_paid_license_date
        , MAX(CASE WHEN TRIAL THEN issued_date ELSE NULL END)     AS last_trial_license_date
        , MAX(ACCOUNT_SFID) AS account_sfid
        , MAX(ACCOUNT_NAME) AS account_name
        , MAX(MASTER_ACCOUNT_SFID) AS master_account_sfid
        , MAX(MASTER_ACCOUNT_NAME) AS master_account_name
        , MAX(COMPANY)             AS company
        , MAX(CASE WHEN NOT TRIAL THEN expire_date ELSE NULL END) AS paid_license_expire_date
        , MAX(CASE WHEN TRIAL THEN expire_date ELSE NULL END) AS trial_license_expire_date
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
    api_request_trial_events AS (
      SELECT
          server_id
        , sum(total_events) as api_request_trial_events_alltime
      FROM {{ ref('user_events_by_date') }}
      WHERE event_name = 'api_request_trial_license'
      GROUP BY 1
    ),
  last_server_date AS (
    SELECT
        server_id
      , MAX(CASE WHEN total_events > 0 THEN date ELSE NULL END) AS last_event_date
      , SUM(signup_events) AS signup_events_alltime
      , SUM(signup_email_events) AS signup_email_events_alltime
      , SUM(post_events)         AS post_events_alltime
      , SUM(admin_events)        AS admin_events_alltime
      , SUM(tutorial_events)     AS tutorial_events_alltime
      , SUM(invite_members_events) AS invite_members_events_alltime
      , MAX(dau_total)           AS max_active_users
      , MAX(MAU_TOTAL)           AS max_mau
      , COUNT(DISTINCT CASE WHEN total_events > 0 THEN date ELSE NULL END) AS days_active
      , COUNT(DISTINCT CASE WHEN COALESCE(total_events,0) = 0 THEN date ELSE NULL END) AS days_inactive
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
  server_fact_prep AS (
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
      , MAX(licenses.account_sfid)                        AS account_sfid
      , MAX(licenses.account_name)                        AS account_name
      , MAX(licenses.master_account_sfid)                 AS master_account_sfid
      , MAX(licenses.master_account_name)                 AS master_account_name
      , MAX(licenses.company)                             AS company
      , MAX(s2.license_id1)                               AS last_license_id1
      , MAX(S2.license_id2)                               AS last_license_id2
      , MAX(licenses.first_paid_license_date)             AS first_paid_license_date
      , MAX(licenses.last_paid_license_date)              AS last_paid_license_date
      , MAX(licenses.paid_license_expire_date)            AS paid_license_expire_date
      , MAX(licenses.first_trial_license_date)            AS first_trial_license_date
      , MAX(licenses.last_trial_license_date)             AS last_trial_license_date
      , MAX(licenses.trial_license_expire_date)           AS trial_license_expire_date
      , MAX(server_details.first_active_date)             AS first_active_date
      , MAX(server_details.last_active_date)              AS last_active_date
      , CASE WHEN COALESCE(MAX(lsd.max_active_users),0) >= 
          COALESCE(MAX(server_details.max_active_user_count),0)
          THEN COALESCE(MAX(lsd.max_active_users),0) 
          ELSE COALESCE(MAX(server_details.max_active_user_count),0)
          END                                             AS max_active_user_count
      , MAX(CASE WHEN COALESCE(max_monthly_active_users, 0) >= 
            COALESCE(max_mau,0) THEN max_monthly_active_users
            ELSE max_mau end)                             AS max_mau
      , MAX(server_details.max_registered_users)          AS max_registered_users
      , MAX(max_registered_deactivated_users)             AS max_registered_deactivated_users
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
      , MAX(server_details.first_100reg_users_date)       AS first_100reg_users_date
      , MAX(server_details.first_500reg_users_date)       AS first_500reg_users_date
      , MAX(server_details.first_1kreg_users_date)        AS first_1kreg_users_date
      , MAX(server_details.first_2500reg_users_date)      AS first_2500reg_users_date
      , MAX(server_details.first_5kreg_users_date)        AS first_5kreg_users_date
      , MAX(server_details.first_10kreg_users_date)       AS first_10kreg_users_date
      , MAX(lsd.post_events_alltime)                      AS posts_events_alltime
      , MAX(lsd.invite_members_events_alltime)            AS invite_members_alltime
      , MAX(lsd.signup_events_alltime)                    AS signup_events_alltime
      , MAX(lsd.signup_email_events_alltime)              AS signup_email_events_alltime
      , MAX(lsd.tutorial_events_alltime)                  AS tutorial_events_alltime
      , MAX(lsd.admin_events_alltime)                     AS admin_events_alltime
      , MAX(lsd.days_active)                              AS days_active
      , MAX(lsd.days_inactive)                            AS days_inactive
      , MAX(server_details.max_posts)                     AS max_posts
      , MAX(api.api_request_trial_events_alltime)         AS api_request_trial_events_alltime
      , MAX(server_daily_details.installation_id)         AS installation_id
    FROM server_details
        LEFT JOIN {{ ref('server_daily_details') }}
            ON server_details.server_id = server_daily_details.server_id
            AND (server_details.last_active_date = server_daily_details.date)
        LEFT JOIN {{ ref('server_daily_details') }} s2
            ON server_details.server_id = s2.server_id
            AND server_details.last_active_license_date = s2.date
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
        LEFT JOIN last_server_date lsd
            ON server_details.server_id = lsd.server_Id
        LEFT JOIN api_request_trial_events api
            ON server_details.server_id = api.server_id
        {{ dbt_utils.group_by(n=1) }}
    ),

    server_fact AS (
      SELECT 
          server_id
        , version
        , first_server_version
        , server_edition
        , first_server_edition
        , first_telemetry_active_date
        , last_telemetry_active_date
        , first_mm2_telemetry_date
        , last_mm2_telemetry_date
        , version_upgrade_count
        , edition_upgrade_count
        , gitlab_install
        , account_sfid
        , account_name
        , master_account_sfid
        , master_account_name
        , company
        , last_license_id1
        , last_license_id2
        , first_paid_license_date
        , last_paid_license_date
        , paid_license_expire_date
        , first_trial_license_date
        , last_trial_license_date
        , trial_license_expire_date
        , first_active_date
        , last_active_date
        , max_active_user_count
        , max_mau
        , max_registered_users
        , max_registered_deactivated_users
        , last_telemetry_active_user_date
        , last_event_active_user_date
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
        , users
        , last_active_license_date
        , nps_users
        , nps_score
        , promoters
        , detractors
        , passives
        , avg_nps_user_score
        , first_100reg_users_date
        , first_500reg_users_date
        , first_1kreg_users_date
        , first_2500reg_users_date
        , first_5kreg_users_date
        , first_10kreg_users_date
        , posts_events_alltime
        , invite_members_alltime
        , signup_events_alltime
        , signup_email_events_alltime
        , tutorial_events_alltime
        , admin_events_alltime
        , days_active
        , days_inactive
        , max_posts
        , MIN(first_active_date) OVER (PARTITION BY COALESCE(ACCOUNT_SFID, LOWER(COMPANY), SERVER_ID)) AS customer_first_active_date
        , MIN(first_paid_license_date) OVER (PARTITION BY COALESCE(ACCOUNT_SFID, LOWER(COMPANY), SERVER_ID)) AS customer_first_paid_license_date
        , api_request_trial_events_alltime
      FROM server_fact_prep
    )
SELECT *
FROM server_fact