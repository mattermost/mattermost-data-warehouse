

with source as (
    select * from {{ source('mm_plugin_prod', 'nps_nps_feedback') }}
),
renamed as (
    select
        -- Common event columns
        id               as event_id
        , event_text     as event_name
        , event          as event_table

        -- Using coalesce here, validated the data, there is no data where both are not null
        -- It was probably a field that was in the earlier versions of Rudder
        -- that changed when we upgraded Rudder to a newer version
        , COALESCE(useractualid, user_actual_id) as user_id
        , user_id        as server_id
        , received_at    as received_at
        , timestamp      as timestamp
        , timestamp::date      as event_date
        , user_create_at as user_created_at

        -- Server info
        , anonymous_id as anonymous_id
        , context_ip as server_ip

        -- NPS data
        , license_id as license_id
        , license_sku as license_sku
        , user_role as user_role
        , feedback as feedback
        , server_install_date as server_install_date
        , email as user_email

        -- Using coalesce here, validated the data, there is no data where both are not null
        -- Same as useractualid above
        , COALESCE(pluginid, plugin_id) as plugin_id
        , COALESCE(serverversion, server_version) as server_version_full
        , COALESCE(pluginversion, plugin_version) as plugin_version

        -- Metadata from Rudderstack
        , context_library_version as context_library_version
        , context_library_name as context_library_name
        , sent_at as sent_at
        , original_timestamp as original_timestamp

        -- Ignored -- Always same value
        -- , context_destination_type
        -- , context_source_id
        -- , context_source_type
        -- , context_destination_id
        --  Ignored - Always same as context_ip
        -- , context_request_ip
        -- Ignored - used by Rudderstack for debugging purposes
        -- , uuid_ts

    from source

)

select * from renamed

