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
        , try_to_decimal(split_part(version, '.', 1)) as version_major
        , try_to_decimal(split_part(version, '.', 2)) as version_minor
        , try_to_decimal(split_part(version, '.', 3)) as version_patch
        , case
            -- Handle special case of community migration
            when user_id = '{{ var("community_server_id") }}' and coalesce(context_traits_installationid, context_traits_installation_id) is not null then 'rxocmq9isjfm3dgyf4ujgnfz3c'
            else coalesce(context_traits_installationid, context_traits_installation_id)
        end as installation_id
        , installation_type
        , anonymous_id
        , context_ip as server_ip

        -- Deployment info
        , operating_system
        , database_type
        , database_version
        , case
            when edition = 'true' then true
            when edition = 'false' then false
            else null
        end as edition

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
    where
        -- Clean up invalid data
        id is not null
)

select
    renamed.*
from
    renamed
    -- Filter installation id/server id pairs that are blacklisted
    left join {{ ref('server_blacklist') }} sb
        on renamed.server_id = sb.server_id and renamed.installation_id = sb.installation_id
where
    sb.server_id is null
