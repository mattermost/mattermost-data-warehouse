

with source as (

    select * from {{ source('mattermost2', 'activity') }}

),

renamed as (

    select
        -- Common event columns
        id               as event_id
        , event          as event_table
        , event_text     as event_name
        , user_id        as server_id
        , received_at    as received_at
        , timestamp      as timestamp

        -- Channel metrics
        , public_channels as count_public_channels
        , public_channels_deleted as count_deleted_public_channels
        , private_channels as count_private_channels
        , private_channels_deleted as count_deleted_private_channels
        , bot_posts_previous_day as count_bot_posts_previous_day
        , posts_previous_day as count_posts_previous_day
        , teams as count_teams
        , slash_commands as count_slash_commands
        , direct_message_channels as count_direct_message_channels
        , posts as count_posts
        , incoming_webhooks as count_incoming_webhooks
        , outgoing_webhooks as count_outgoing_webhooks

         -- User & account metrics
        , coalesce(active_users_daily, active_users) as daily_active_users
        , active_users_monthly as monthly_active_users
        , registered_users as count_registered_users
        , coalesce(registered_deactivated_users, registered_inactive_users) as count_registered_deactivated_users
        , bot_accounts as bot_accounts
        , guest_accounts as guest_accounts

        -- Metadata from Segment
        , context_library_version
        , context_library_name
        , sent_at
        , original_timestamp

        -- Ignored - legacy feature with only ~ 5.7% of segment data having value
        -- , used_apiv3
        -- Ignored - used by segment for debugging purposes
        -- , uuid_ts

    from source

)

select * from renamed

