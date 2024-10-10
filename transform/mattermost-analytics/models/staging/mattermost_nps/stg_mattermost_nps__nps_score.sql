with source as (
    select * from {{ source('mattermost_nps', 'nps_score') }}
) select     
        -- Common event columns
        id                    as event_id
        , event_text          as event_name
        , event               as event_table
        , user_actual_id      as user_id 
        , user_id             as server_id 
        , received_at         as received_at
        , timestamp           as timestamp
        , timestamp::date     as event_date
 
        -- NPS columns
        , user_create_at      as user_created_at
        , license_id          as license_id
        , license_sku         as license_sku
        , user_role           as user_role
        , score               as score
        , server_install_date as server_install_date
        , server_version      as server_version_full
         
        -- Metadata from Segment
        , context_library_version as context_library_version
        , context_library_name as context_library_name
        , sent_at             as sent_at
        , original_timestamp  as original_timestamp
        --, server_id           as server_id
        --uuid_ts
    from 
        source