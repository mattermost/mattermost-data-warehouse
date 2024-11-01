
with source as (
    select * from {{ source('mattermost2', 'config_plugin') }}
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

        , allow_insecure_download_url
        , automatic_prepackaged_plugins
        , enable         as enable_plugin
        , enable_antivirus
        , enable_autolink
        , enable_aws_sns
        , enable_confluence
        , enable_custom_user_attributes
        , enable_github
        , enable_gitlab
        , enable_health_check
        , enable_jenkins
        , enable_jira
        , enable_jitsi
        , enable_marketplace
        , enable_mscalendar
        , enable_nps
        , enable_nps_survey
        , enable_remote_marketplace
        , enable_skype4business
        , enable_todo
        , enable_uploads
        , enable_webex
        , enable_welcome_bot
        , enable_zoom
        , is_default_marketplace_url
        , require_plugin_signature
        , signature_public_key_files
        , version_antivirus
        , version_autolink
        , version_aws_sns
        , version_custom_user_attributes
        , version_github
        , version_gitlab
        , version_jenkins
        , version_jira
        , version_nps
        , version_webex
        , version_welcome_bot
        , version_zoom
        
        -- Metadata from Segment
        , context_library_name
        , context_library_version
        , sent_at
        , try_to_timestamp_ntz(original_timestamp) as original_timestamp

        -- Ignored - used by Segment for debugging purposes
        -- , uuid_ts
    from source
)

select * from renamed
