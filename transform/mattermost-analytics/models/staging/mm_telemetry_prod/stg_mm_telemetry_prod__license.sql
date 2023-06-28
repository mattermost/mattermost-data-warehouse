
with source as (

    select * from {{ source('mm_telemetry_prod', 'license') }}

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

        -- License info
        , customer_id
        , license_id
        , edition as sku_short_name
        , to_timestamp_ntz(expire / 1000) as expire_at
        , to_timestamp_ntz(issued / 1000) as issued_at
        , users

        -- Server info
        , anonymous_id
        , coalesce(context_traits_installationid,  context_traits_installation_id) as installation_id
        , context_ip as server_ip

        -- Features
        , feature_advanced_logging
        , feature_cloud
        , feature_cluster
        , feature_compliance
        , feature_custom_permissions_schemes
        , feature_data_retention
        , feature_elastic_search
        , feature_email_notification_contents
        , feature_enterprise_plugins
        , feature_future
        , feature_google
        , feature_guest_accounts
        , feature_guest_accounts_permissions
        , feature_id_loaded
        , feature_ldap
        , feature_ldap_groups
        , feature_lock_teammate_name_display
        , feature_message_export
        , feature_metrics
        , feature_mfa
        , feature_mhpns
        , coalesce(feature_office365, feature_office_365) as feature_office_365
        , feature_openid
        , feature_remote_cluster_service
        , feature_saml
        , feature_shared_channels

        -- Metadata from rudderstack
        , context_library_version
        , context_library_name
        , uuid_ts
        , sent_at
        , original_timestamp

        -- Ignored - Always null
        -- , channel
        -- Ignored -- No useful information
        -- , context_destination_type
        -- , context_source_id
        -- , context_source_type
        -- , context_destination_id
        --  Ignored - Always same as context_ip
        -- , context_request_ip
        -- Ignored - undocumented and missing from code
        -- , _start

    from source

)

select * from renamed
