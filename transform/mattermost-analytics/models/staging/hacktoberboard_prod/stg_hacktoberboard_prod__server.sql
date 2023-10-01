
with source as (

    select * from {{ source('hacktoberboard_prod', 'server') }}

),

renamed as (

    select
        -- Common event columns
        id               as event_id
        , event          as event_table
        , event_text     as event_name
        , user_id        as telemetry_id
        , received_at    as received_at
        , timestamp      as timestamp

        -- Server info
        , anonymous_id
        , context_ip as server_ip

        , version
        , build_number
        , build_hash
        , edition
        , operating_system
        , server_id

        -- Metadata from Rudderstack
        , context_library_version
        , context_library_name
        , sent_at
        , original_timestamp

        -- Ignored - always same value
        -- , context_destination_type
        -- , context_source_type
        -- , context_destination_id
        -- , context_source_id
        --  Ignored - Always same as context_ip
        -- , context_request_ip
        -- Ignored - used by Rudderstack for debugging purposes
        -- , uuid_ts

    from source

)

select * from renamed
