

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

        -- License info
        , customer_id
        , license_id
        , edition as sku_short_name
        , to_timestamp_ntz(expire / 1000) as expire_at
        , to_timestamp_ntz(issued / 1000) as issued_at
        , users

        -- Features
        , feature_cluster
        , feature_compliance
        , feature_custom_brand
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
        , feature_office365 as feature_office_365
        , feature_password
        , feature_saml

        -- Metadata from Segment
        , context_library_version
        , context_library_name
        , uuid_ts
        , sent_at
        , original_timestamp
        -- Ignored - undocumented and missing from code
        -- , _start
    from source

)

select * from renamed
