with source as (
    select * from {{ source('mattermost2', 'server') }}
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

    from source
)
select
    *
from
    renamed
