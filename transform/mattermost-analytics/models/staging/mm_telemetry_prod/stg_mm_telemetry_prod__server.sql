with source as (
    select * from {{ source('mm_telemetry_prod', 'server') }}
), renamed as (
    select

        -- Common event columns
        id               as event_id
        , event          as event_table
        , event_text     as event_name
        , user_id        as server_id
        , received_at    as received_at
        , timestamp      as timestamp

        -- Server info
        , system_admins  as count_system_admins
        , version        as version_full
        , split_part(version, '.', 1) as version_major
        , split_part(version, '.', 2) as version_minor
        , split_part(version, '.', 3) as version_patch
        , coalesce(context_traits_installationid,  context_traits_installation_id) as installation_id
        , installation_type
        , anonymous_id
        , context_ip as server_ip

        -- Deployment info
        , operating_system
        , database_type
        , database_version
        , edition

        -- Metadata from segment
        , context_library_version
        , context_library_name
        , uuid_ts
        , sent_at
        , original_timestamp

        -- Ignored - Always null
        -- , channel
        -- Ignored -- Always same value
        -- , context_destination_type
        -- , context_source_id
        -- , context_source_type
        -- , context_destination_id
        --  Ignored - Always same as context_ip
        -- , context_request_ip
        -- Ignored - not reflected in code
        -- , server_id
    from source

)

select * from renamed
