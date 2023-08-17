

with source as (
    select * from {{ source('mm_telemetry_prod', 'activity') }}
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

        -- Server info
        , coalesce(context_traits_installationid,  context_traits_installation_id) as installation_id
        , anonymous_id
        , context_ip as server_ip

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
        , active_users_daily as daily_active_users
        , active_users_monthly as monthly_active_users
        , registered_users as count_registered_users
        , registered_deactivated_users as count_registered_deactivated_users
        , bot_accounts as bot_accounts
        , guest_accounts as guest_accounts

        -- Metadata from Rudderstack
        , context_library_version
        , context_library_name
        , sent_at
        , original_timestamp

        -- Ignored - Always null
        -- , channel
        -- , daily_active_users
        -- , weekly_active_users
        -- , monthly_active_users
        -- Ignored -- Always same value
        -- , context_destination_type
        -- , context_source_id
        -- , context_source_type
        -- , context_destination_id
        -- Ignored, > 98% null
        -- , storage_bytes
        --  Ignored - Always same as context_ip
        -- , context_request_ip
        -- Ignored - not reflected in code
        -- , server_id
        -- Ignored - used by Rudderstack for debugging purposes
        -- , uuid_ts

    from source

)

select * from renamed

