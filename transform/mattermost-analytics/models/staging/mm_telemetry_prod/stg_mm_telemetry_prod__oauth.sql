
with source as (
    select * from {{ source('mm_telemetry_prod', 'config_oauth') }}
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

        -- OAuth information
        , coalesce(enable_office365, enable_office_365) as is_office365_enabled
        , enable_google as is_google_enabled
        , enable_gitlab as is_gitlab_enabled
        , enable_openid as is_openid_enabled
        , openid_google as is_openid_google_enabled
        , openid_gitlab as is_openid_gitlab_enabled
        , coalesce(openid_office365, openid_office_365) as is_openid_office365_enabled

       -- Ignored - Always null
        -- , channel
        -- Metadata from Rudderstack
        , context_library_version
        , context_library_name
        , sent_at
        , original_timestamp

        -- Ignored -- Always same value
        -- , context_destination_type
        -- , context_source_id
        -- , context_source_type
        -- , context_destination_id
        -- Ignored - used by Rudderstack for debugging purposes
        -- , uuid_ts
    from source
)

select * from renamed
