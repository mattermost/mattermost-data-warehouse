with source as (

    select * from {{ source('mattermost2', 'license') }}

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
        , cast(timestamp as date) as license_telemetry_date

        -- IDs
        , license_id
        , customer_id

        -- License information
        , edition as license_name
        , users as licensed_seats
        , try_to_timestamp_ntz(cast(issued as varchar)) as issued_at
        , try_to_timestamp_ntz(cast(expire as varchar)) as expire_at
        , try_to_timestamp_ntz(cast(_start as varchar)) as starts_at

        -- Features
        , feature_cluster as is_feature_cluster_enabled
        , feature_compliance as is_feature_compliance_enabled
        , feature_custom_brand as is_feature_custom_brand_enabled
        , feature_custom_permissions_schemes as is_feature_custom_permissions_schemes_enabled
        , feature_data_retention as is_feature_data_retention_enabled
        , feature_elastic_search as is_feature_elastic_search_enable
        , feature_email_notification_contents as is_feature_email_notification_contents_enabled
        , feature_enterprise_plugins as is_feature_enterprise_plugins_enabled
        , feature_future as is_feature_future_enabled
        , feature_google as is_feature_google_enabled
        , feature_guest_accounts as is_feature_guest_accounts_enabled
        , feature_guest_accounts_permissions as is_feature_guest_accounts_permissions_enabled
        , feature_id_loaded as is_feature_id_loaded_enabled
        , feature_ldap as is_feature_ldap_enabled
        , feature_ldap_groups as is_feature_ldap_groups_enabled
        , feature_lock_teammate_name_display as is_feature_lock_teammate_name_display_enabled
        , feature_message_export as is_feature_message_export_enabled
        , feature_metrics as is_feature_metrics_enabled
        , feature_mfa as is_feature_mfa_enabled
        , feature_mhpns as is_feature_mhpns_enabled
        , feature_office365 as is_feature_office365_enabled
        , feature_password as is_feature_password_enabled
        , feature_saml as is_feature_saml_enabled

        -- Metadata from Segment
        , context_library_version
        , context_library_name
        , sent_at
        , original_timestamp

        -- Ignored - used by segment for debugging purposes
        -- , uuid_ts
    from source

)

select * from renamed