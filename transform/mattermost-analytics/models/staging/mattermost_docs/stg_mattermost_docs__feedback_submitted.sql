with source as (

    select * from {{ source('mattermost_docs', 'feedback_submitted') }}

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
        , anonymous_id
        , channel

        -- Event properties
        , rating
        , feedback
        , label

        -- Contextual properties
        , context_app_version
        , context_screen_width
        , context_page_path
        , context_page_initial_referrer
        , context_page_initial_referring_domain
        , context_locale
        , context_user_agent
        , context_page_referring_domain
        , context_page_url
        , context_screen_height
        , context_screen_inner_height
        , context_page_tab_url
        , context_screen_inner_width
        , context_page_referrer
        , context_app_build
        , context_app_namespace
        , context_page_title
        , context_request_ip
        , context_app_name
        , context_screen_density
        , context_ip
        , context_traits_portal_customer_id
        , context_page_search
        , context_campaign_source
        , context_campaign_name
        , context_campaign_medium
        , context_traits_use_oauth
        , context_traits_auth_provider
        , context_session_id
        , context_session_start
        , context_campaign_content

        -- Metadata
        , context_library_version
        , context_library_name
        , uuid_ts
        , sent_at
        , original_timestamp

        -- Ignored -- Always same value
        -- , context_destination_type
        -- , context_source_id
        -- , context_source_type
        -- , context_destination_id

    from source

)

select * from renamed
