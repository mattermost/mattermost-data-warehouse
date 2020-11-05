{{config({
    'materialized': 'incremental',
    'schema': 'mattermost',
    'tags':'hourly',
    'unique_key':'server_id'
  })
}}

WITH server_details AS (
    SELECT
        server_id
      , MAX(CASE WHEN coalesce(active_users_daily, active_users) > active_user_count 
              THEN coalesce(active_users_daily, active_users)
              ELSE active_user_count END) AS                                              max_active_user_count
      , MAX(active_users_monthly) AS                                                      max_monthly_active_users
      , MAX(CASE WHEN COALESCE(registered_users,0) > COALESCE(user_count, 0)
            THEN COALESCE(registered_users,0) 
            ELSE COALESCE(user_count,0) END)                                          AS  max_registered_users
      , MAX(coalesce(registered_deactivated_users, 0))                                AS  max_registered_deactivated_users
      , MAX(CASE WHEN active_user_count > 0 or coalesce(active_users_daily, active_users) > 0 THEN date ELSE NULL END) AS                   last_active_user_date
      , MIN(CASE WHEN version IS NOT NULL THEN date ELSE NULL END) AS                     first_server_version_date
      , MIN(CASE WHEN edition IS NOT NULL THEN date ELSE NULL END)                     AS first_edition_date
      , MAX(CASE WHEN edition IS NOT NULL THEN date ELSE NULL END)                     AS last_edition_date
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

    server_activity as (
      SELECT 
        COALESCE(r.user_id, s.user_id) as user_id
        , max(COALESCE(r.registered_users, s.registered_users)) as registered_users
        , max(COALESCE(r.registered_deactivated_users, s.registered_deactivated_users)) as registered_deactivated_users
        , max(COALESCE(r.posts, s.posts)) as posts
        , max(COALESCE(r.direct_message_channels, s.direct_message_channels)) as direct_message_channels
        , max(COALESCE(r.public_channels - r.public_channels_deleted, s.public_channels - s.public_channels_deleted)) as public_channels
        , max(COALESCE(r.private_channels - r.private_channels_deleted, s.private_channels - s.private_channels_deleted)) as private_channels
        , max(COALESCE(r.slash_commands, s.slash_commands)) as slash_commands
        , max(COALESCE(r.teams, s.teams)) AS teams
        , MAX(COALESCE(r.posts_previous_day, s.posts_previous_day)) as posts_previous_day
        , MAX(COALESCE(r.bot_posts_previous_day, s.bot_posts_previous_day)) as bot_posts_previous_day
        , max(COALESCE(r.active_users_daily, s.active_users, s.active_users_daily)) as active_users
        , max(COALESCE(r.active_users_monthly, s.active_users_monthly)) as monthly_active_users
        , max(COALESCE(r.bot_accounts, s.bot_accounts)) as bot_accounts
        , max(COALESCE(r.guest_accounts, s.guest_accounts)) as guest_accounts
        , max(COALESCE(r.incoming_webhooks, s.incoming_webhooks)) as incoming_webhooks
        , max(COALESCE(r.outgoing_webhooks, s.outgoing_webhooks)) as outgoing_webhooks
        , max(COALESCE(r.max_time, s.max_time)) AS max_timestamp
      FROM (
            SELECT * 
            FROM {{ source('mm_telemetry_prod', 'activity') }} s1
            JOIN (select user_id as server_id, max(timestamp) as max_time from {{ source('mm_telemetry_prod', 'activity') }} group by 1) s2
            ON s1.user_id = s2.server_id AND s1.timestamp = s2.max_time
          ) r
      FULL OUTER JOIN
          (
            SELECT * 
            FROM {{ source('mattermost2', 'activity') }} s1
            JOIN (select user_id as server_id, max(timestamp) as max_time from {{ source('mattermost2', 'activity') }} group by 1) s2
            ON s1.user_id = s2.server_id AND s1.timestamp = s2.max_time
          ) s
        ON r.user_id = s.user_id and r.timestamp::date = s.timestamp::date
      
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
        COALESCE(user_id, context_server, context_traits_server) AS server_id
      , MAX(TIMESTAMP::DATE) AS last_event_date
      , COUNT(CASE WHEN category = 'signup' THEN id ELSE NULL END) AS signup_events_alltime
      , COUNT(CASE WHEN category = 'signup_email' THEN id ELSE NULL END) AS signup_email_events_alltime
      , COUNT(CASE WHEN COALESCE(type, event) = 'api_posts_create' THEN id ELSE NULL END)         AS post_events_alltime
      , COUNT(CASE WHEN category = 'admin' THEN id ELSE NULL END)        AS admin_events_alltime
      , COUNT(CASE WHEN category = 'tutorial' THEN id ELSE NULL END)     AS tutorial_events_alltime
      , COUNT(CASE WHEN COALESCE(type, event) IN ('click_invite_members','click_copy_invite_link','api_teams_invite_members') THEN id ELSE NULL END) AS invite_members_events_alltime
      , COUNT(DISTINCT timestamp::DATE) AS days_active
      , DATEDIFF(DAY, MIN(TIMESTAMP::DATE), CURRENT_DATE) - COUNT(DISTINCT TIMESTAMP::DATE) AS days_inactive
    FROM {{ ref('user_events_telemetry') }}
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
        sdd.server_id
      , MAX(server_daily_details.version)                 AS version
      , MAX(fse.first_server_version)          AS first_server_version
      , MAX(fse.edition)                                  AS server_edition
      , MAX(fse.first_server_edition)                     AS first_server_edition
      , MAX(sdd.first_telemetry_active_date)   AS first_telemetry_active_date
      , MAX(sdd.last_telemetry_active_date)    AS last_telemetry_active_date
      , MAX(sdd.first_mm2_telemetry_date)      AS first_mm2_telemetry_date
      , MAX(sdd.last_mm2_telemetry_date)       AS last_mm2_telemetry_date
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
      , MAX(sdd.first_active_date)             AS first_active_date
      , MAX(sdd.last_active_date)              AS last_active_date
      , COALESCE(MAX(server_details.max_active_user_count),0)
                                                          AS max_active_user_count
      , MAX(COALESCE(max_monthly_active_users, 0))                             AS max_mau
      , MAX(lsd.last_event_date)         AS last_telemetry_active_user_date
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
      , MAX(sdd.last_active_license_date)      AS last_active_license_date
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
      , MAX(api.api_request_trial_events_alltime)         AS api_request_trial_events_alltime
      , MAX(sdd.installation_id)         AS installation_id
      , MAX(sdd.installation_type)       AS installation_type
      , max(registered_users) as registered_users
        , max(server_activity.registered_deactivated_users) as registered_deactivated_users
        , max(server_activity.posts) as posts
        , max(server_activity.direct_message_channels) as direct_message_channels
        , max(server_activity.public_channels) as public_channels
        , max(server_activity.private_channels) as private_channels
        , max(server_activity.slash_commands) as slash_commands
        , max(server_activity.teams) AS teams
        , MAX(server_activity.posts_previous_day) as posts_previous_day
        , MAX(server_activity.bot_posts_previous_day) as bot_posts_previous_day
        , max(server_activity.active_users) as active_users
        , max(server_activity.monthly_active_users) as monthly_active_users
        , max(server_activity.bot_accounts) as bot_accounts
        , max(server_activity.guest_accounts) as guest_accounts
        , max(server_activity.incoming_webhooks) as incoming_webhooks
        , max(server_activity.outgoing_webhooks) as outgoing_webhooks
    FROM (
            SELECT 
              server_id
            , MIN(CASE WHEN in_security OR in_mm2_server THEN date ELSE NULL END) AS            first_active_date
            , MAX(CASE WHEN in_security OR in_mm2_server THEN date ELSE NULL END) AS            last_active_date
            , MIN(CASE WHEN in_security THEN date ELSE NULL END) AS                             first_telemetry_active_date
            , MAX(CASE WHEN in_security THEN date ELSE NULL END) AS                             last_telemetry_active_date
            , MAX(installation_id) AS installation_id
            , MAX(installation_type) AS installation_type
            , MAX(CASE
                WHEN license_id1 IS NOT NULL OR license_id2 IS NOT NULL THEN date
                                                                        ELSE NULL END) AS last_active_license_date
            , MIN(CASE WHEN license_id1 IS NOT NULL OR license_id2 IS NOT NULL THEN date
                                                                        ELSE NULL END) AS first_active_license_date
            , MIN(CASE WHEN in_mm2_server THEN date ELSE NULL END)                           AS first_mm2_telemetry_date
            , MAX(CASE WHEN in_mm2_server THEN date ELSE NULL END)                           AS last_mm2_telemetry_date
            FROM {{ ref('server_daily_details') }}
            GROUP BY 1
          ) sdd
        LEFT JOIN server_details
          ON sdd.server_id = server_details.server_id
        LEFT JOIN {{ ref('server_daily_details') }}
            ON sdd.server_id = server_daily_details.server_id
            AND (server_daily_details.date >= sdd.last_active_date)
        LEFT JOIN {{ ref('server_daily_details') }} s2
            ON sdd.server_id = s2.server_id
            AND sdd.last_active_license_date = s2.date
        LEFT JOIN {{ ref('nps_server_daily_score') }} nps
            ON sdd.server_id = nps.server_id
            AND nps.date = DATE_TRUNC('day', CURRENT_DATE - INTERVAL '1 DAY')
        LEFT JOIN server_upgrades upgrades
            ON sdd.server_id = upgrades.server_id
        LEFT JOIN server_active_users sau
            ON sdd.server_id = sau.server_id
        LEFT JOIN first_server_edition fse
            ON sdd.server_id = fse.server_id
        LEFT JOIN licenses
            ON sdd.server_id = licenses.server_id
        LEFT JOIN {{ ref('server_oauth_details') }} oauth
            ON sdd.server_id = oauth.server_id
            AND sdd.first_mm2_telemetry_date = oauth.date
        LEFT JOIN last_server_date lsd
            ON sdd.server_id = lsd.server_Id
        LEFT JOIN api_request_trial_events api
            ON sdd.server_id = api.server_id
        LEFT JOIN server_activity
            ON sdd.server_id = server_activity.user_id
        {% if is_incremental() %}
          WHERE sdd.last_active_date >= (SELECT MAX(last_active_date) FROM {{this}})
        {% endif %}
        {{ dbt_utils.group_by(n=1) }}
    ),

    server_fact AS (
      SELECT 
          *
        , MIN(first_active_date) OVER (PARTITION BY COALESCE(ACCOUNT_SFID, LOWER(COMPANY), SERVER_ID)) AS customer_first_active_date
        , MIN(first_paid_license_date) OVER (PARTITION BY COALESCE(ACCOUNT_SFID, LOWER(COMPANY), SERVER_ID)) AS customer_first_paid_license_date
      FROM server_fact_prep
    )
SELECT *
FROM server_fact