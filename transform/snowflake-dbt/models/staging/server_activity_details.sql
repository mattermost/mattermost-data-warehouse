{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp               AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'activity') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_activity_details AS (
         SELECT
             a.timestamp::DATE                 AS date
           , a.user_id                         AS server_id
           , MAX(active_users)                 AS active_users
           , MAX(active_users_daily)           AS active_users_daily
           , MAX(active_users_monthly)         AS active_users_monthly
           , MAX(bot_accounts)                 AS bot_accounts
           , MAX(bot_posts_previous_day)       AS bot_posts_previous_day
           , MAX(direct_message_channels)      AS direct_message_channels
           , MAX(incoming_webhooks)            AS incoming_webhooks
           , MAX(outgoing_webhooks)            AS outgoing_webhooks
           , MAX(posts)                        AS posts
           , MAX(posts_previous_day)           AS posts_previous_day
           , MAX(private_channels)             AS private_channels
           , MAX(private_channels_deleted)     AS private_channels_deleted
           , MAX(public_channels)              AS public_channels
           , MAX(public_channels_deleted)      AS public_channels_deleted
           , MAX(registered_deactivated_users) AS registered_deactivated_users
           , MAX(registered_inactive_users)    AS registered_inactive_users
           , MAX(registered_users)             AS registered_users
           , MAX(slash_commands)               AS slash_commands
           , MAX(teams)                        AS teams
           , MAX(used_apiv3)                   AS used_apiv3
         FROM {{ source('mattermost2', 'activity') }} a
              JOIN max_timestamp       mt
                   ON a.user_id = mt.user_id
                       AND a.timestamp = mt.max_timestamp
         GROUP BY 1, 2)
SELECT *
FROM server_activity_details;