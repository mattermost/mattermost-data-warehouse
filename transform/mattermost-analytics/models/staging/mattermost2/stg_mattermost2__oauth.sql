with source as (
    select * from {{ source('mattermost2', 'config_oauth') }}
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

        , enable_office365 as is_office365_enabled
        , enable_google as is_google_enabled
        , enable_gitlab as is_gitlab_enabled

        -- Metadata from Segment
        , context_library_name
        , context_library_version
        , sent_at
        , try_to_timestamp_ntz(original_timestamp) as original_timestamp

        -- Ignored - used by segment for debugging purposes
        -- , uuid_ts

    from source
)

select * from renamed
