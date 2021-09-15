{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp        AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_team') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_team') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_team_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , MAX(COALESCE(r.enable_confirm_notifications_to_channel, s.enable_confirm_notifications_to_channel))   AS enable_confirm_notifications_to_channel
           , MAX(COALESCE(r.enable_custom_brand, s.enable_custom_brand))                       AS enable_custom_brand
           , MAX(COALESCE(r.enable_open_server, s.enable_open_server))                        AS enable_open_server
           , MAX(COALESCE(r.enable_team_creation, s.enable_team_creation))                      AS enable_team_creation
           , MAX(COALESCE(r.enable_user_creation, s.enable_user_creation))                      AS enable_user_creation
           , MAX(COALESCE(r.enable_user_deactivation, s.enable_user_deactivation))                  AS enable_user_deactivation
           , MAX(COALESCE(r.enable_x_to_leave_channels_from_lhs, s.enable_x_to_leave_channels_from_lhs))       AS enable_x_to_leave_channels_from_lhs
           , MAX(COALESCE(r.experimental_default_channels, s.experimental_default_channels))             AS experimental_default_channels
           , MAX(COALESCE(r.experimental_enable_automatic_replies, s.experimental_enable_automatic_replies))     AS experimental_enable_automatic_replies
           , MAX(COALESCE(r.experimental_primary_team, s.experimental_primary_team))                 AS experimental_primary_team
           , MAX(COALESCE(r.experimental_town_square_is_hidden_in_lhs, s.experimental_town_square_is_hidden_in_lhs)) AS experimental_town_square_is_hidden_in_lhs
           , MAX(COALESCE(r.experimental_town_square_is_read_only, s.experimental_town_square_is_read_only))     AS experimental_town_square_is_read_only
           , MAX(COALESCE(r.experimental_view_archived_channels, s.experimental_view_archived_channels))       AS experimental_view_archived_channels
           , MAX(COALESCE(r.isdefault_custom_brand_text, s.isdefault_custom_brand_text))               AS isdefault_custom_brand_text
           , MAX(COALESCE(r.isdefault_custom_description_text, s.isdefault_custom_description_text))         AS isdefault_custom_description_text
           , MAX(COALESCE(r.isdefault_site_name, s.isdefault_site_name))                       AS isdefault_site_name
           , MAX(COALESCE(r.isdefault_user_status_away_timeout, s.isdefault_user_status_away_timeout))        AS isdefault_user_status_away_timeout
           , MAX(COALESCE(r.lock_teammate_name_display, s.lock_teammate_name_display))                AS lock_teammate_name_display
           , MAX(COALESCE(r.max_channels_per_team, s.max_channels_per_team))                     AS max_channels_per_team
           , MAX(COALESCE(r.max_notifications_per_channel, s.max_notifications_per_channel))             AS max_notifications_per_channel
           , MAX(COALESCE(r.max_users_per_team, s.max_users_per_team))                        AS max_users_per_team
           , MAX(COALESCE(r.restrict_direct_message, s.restrict_direct_message))                   AS restrict_direct_message
           , MAX(COALESCE(r.restrict_private_channel_creation, s.restrict_private_channel_creation))         AS restrict_private_channel_creation
           , MAX(COALESCE(r.restrict_private_channel_deletion, s.restrict_private_channel_deletion))         AS restrict_private_channel_deletion
           , MAX(COALESCE(r.restrict_private_channel_management, s.restrict_private_channel_management))       AS restrict_private_channel_management
           , MAX(COALESCE(r.restrict_private_channel_manage_members, s.restrict_private_channel_manage_members))   AS restrict_private_channel_manage_members
           , MAX(COALESCE(r.restrict_public_channel_creation, s.restrict_public_channel_creation))          AS restrict_public_channel_creation
           , MAX(COALESCE(r.restrict_public_channel_deletion, s.restrict_public_channel_deletion))          AS restrict_public_channel_deletion
           , MAX(COALESCE(r.restrict_public_channel_management, s.restrict_public_channel_management))        AS restrict_public_channel_management
           , MAX(COALESCE(r.restrict_team_invite, s.restrict_team_invite))                      AS restrict_team_invite
           , MAX(COALESCE(r.teammate_name_display, s.teammate_name_display))                     AS teammate_name_display
           , MAX(COALESCE(s.view_archived_channels, NULL ))                    AS view_archived_channels           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           , MAX(COALESCE(r.enable_custom_user_statuses, NULL))               AS enable_custom_user_statuses
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_team') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_team') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 35, 36
     )
SELECT *
FROM server_team_details