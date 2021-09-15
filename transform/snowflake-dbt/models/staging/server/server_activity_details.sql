{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp            AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'activity') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp            AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'activity') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

     server_activity_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::date)              AS date
           , COALESCE(s.user_id, r.user_id)                              AS server_id
           , MAX(COALESCE(s.active_users, NULL ))                 AS active_users
           , MAX(COALESCE(s.active_users_daily, r.active_users_daily))           AS active_users_daily
           , MAX(COALESCE(s.active_users_monthly, r.active_users_monthly))         AS active_users_monthly
           , MAX(COALESCE(s.bot_accounts, r.bot_accounts))                 AS bot_accounts
           , MAX(COALESCE(s.bot_posts_previous_day, r.bot_posts_previous_day))       AS bot_posts_previous_day
           , MAX(COALESCE(s.direct_message_channels, r.direct_message_channels))      AS direct_message_channels
           , MAX(COALESCE(s.incoming_webhooks, r.incoming_webhooks))            AS incoming_webhooks
           , MAX(COALESCE(s.outgoing_webhooks, r.outgoing_webhooks))            AS outgoing_webhooks
           , MAX(COALESCE(s.posts, r.posts))                        AS posts
           , MAX(COALESCE(s.posts_previous_day, r.posts_previous_day))           AS posts_previous_day
           , MAX(COALESCE(s.private_channels, r.private_channels))             AS private_channels
           , MAX(COALESCE(s.private_channels_deleted, r.private_channels_deleted))     AS private_channels_deleted
           , MAX(COALESCE(s.public_channels, r.public_channels))              AS public_channels
           , MAX(COALESCE(s.public_channels_deleted, r.public_channels_deleted))      AS public_channels_deleted
           , MAX(COALESCE(s.registered_deactivated_users, r.registered_deactivated_users)) AS registered_deactivated_users
           , MAX(COALESCE(s.registered_inactive_users, NULL ))    AS registered_inactive_users
           , MAX(COALESCE(s.registered_users, r.registered_users))             AS registered_users
           , MAX(COALESCE(s.slash_commands, r.slash_commands))               AS slash_commands
           , MAX(COALESCE(s.teams, r.teams))                        AS teams
           , MAX(COALESCE(s.used_apiv3, NULL ))                   AS used_apiv3
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , MAX(COALESCE(s.guest_accounts, r.guest_accounts))               AS guest_accounts
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'activity') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'activity') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 23, 25
         )
SELECT *
FROM server_activity_details