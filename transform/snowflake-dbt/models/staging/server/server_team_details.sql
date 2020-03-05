{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_team') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_team_details AS (
         SELECT
             timestamp::DATE                                AS date
           , t.user_id                                      AS server_id
           , MAX(enable_confirm_notifications_to_channel)   AS enable_confirm_notifications_to_channel
           , MAX(enable_custom_brand)                       AS enable_custom_brand
           , MAX(enable_open_server)                        AS enable_open_server
           , MAX(enable_team_creation)                      AS enable_team_creation
           , MAX(enable_user_creation)                      AS enable_user_creation
           , MAX(enable_user_deactivation)                  AS enable_user_deactivation
           , MAX(enable_x_to_leave_channels_from_lhs)       AS enable_x_to_leave_channels_from_lhs
           , MAX(experimental_default_channels)             AS experimental_default_channels
           , MAX(experimental_enable_automatic_replies)     AS experimental_enable_automatic_replies
           , MAX(experimental_primary_team)                 AS experimental_primary_team
           , MAX(experimental_town_square_is_hidden_in_lhs) AS experimental_town_square_is_hidden_in_lhs
           , MAX(experimental_town_square_is_read_only)     AS experimental_town_square_is_read_only
           , MAX(experimental_view_archived_channels)       AS experimental_view_archived_channels
           , MAX(isdefault_custom_brand_text)               AS isdefault_custom_brand_text
           , MAX(isdefault_custom_description_text)         AS isdefault_custom_description_text
           , MAX(isdefault_site_name)                       AS isdefault_site_name
           , MAX(isdefault_user_status_away_timeout)        AS isdefault_user_status_away_timeout
           , MAX(lock_teammate_name_display)                AS lock_teammate_name_display
           , MAX(max_channels_per_team)                     AS max_channels_per_team
           , MAX(max_notifications_per_channel)             AS max_notifications_per_channel
           , MAX(max_users_per_team)                        AS max_users_per_team
           , MAX(restrict_direct_message)                   AS restrict_direct_message
           , MAX(restrict_private_channel_creation)         AS restrict_private_channel_creation
           , MAX(restrict_private_channel_deletion)         AS restrict_private_channel_deletion
           , MAX(restrict_private_channel_management)       AS restrict_private_channel_management
           , MAX(restrict_private_channel_manage_members)   AS restrict_private_channel_manage_members
           , MAX(restrict_public_channel_creation)          AS restrict_public_channel_creation
           , MAX(restrict_public_channel_deletion)          AS restrict_public_channel_deletion
           , MAX(restrict_public_channel_management)        AS restrict_public_channel_management
           , MAX(restrict_team_invite)                      AS restrict_team_invite
           , MAX(teammate_name_display)                     AS teammate_name_display
           , MAX(view_archived_channels)                    AS view_archived_channels
         FROM {{ source('mattermost2', 'config_team') }} t
              JOIN max_timestamp      mt
                   ON t.user_id = mt.user_id
                       AND mt.max_timestamp = t.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_team_details