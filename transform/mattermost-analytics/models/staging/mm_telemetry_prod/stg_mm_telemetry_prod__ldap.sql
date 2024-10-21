with source as (
    select * from {{ source('mm_telemetry_prod', 'config_ldap') }}
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

        -- Server info
        , coalesce(context_traits_installationid,  context_traits_installation_id) as installation_id
        , anonymous_id
        , context_ip as server_ip

        , connection_security                     as connection_security_ldap
        , enable                                  as enable_ldap
        , enable_admin_filter
        , enable_sync
        , isdefault_email_attribute               as isdefault_email_attribute_ldap
        , isdefault_first_name_attribute          as isdefault_first_name_attribute_ldap
        , isdefault_group_display_name_attribute
        , isdefault_group_id_attribute
        , isdefault_id_attribute                  as isdefault_id_attribute_ldap
        , isdefault_last_name_attribute           as isdefault_last_name_attribute_ldap
        , isdefault_login_button_border_color     as isdefault_login_button_border_color_ldap
        , isdefault_login_button_color            as isdefault_login_button_color_ldap
        , isdefault_login_button_text_color       as isdefault_login_button_text_color_ldap
        , isdefault_login_field_name
        , isdefault_login_id_attribute
        , isdefault_nickname_attribute            as isdefault_nickname_attribute_ldap
        , isdefault_position_attribute            as isdefault_position_attribute_ldap
        , isdefault_username_attribute            as isdefault_username_attribute_ldap
        , isempty_admin_filter
        , isempty_group_filter
        , isempty_guest_filter
        , isnotempty_picture_attribute
        , isnotempty_private_key
        , isnotempty_public_certificate
        , max_page_size
        , query_timeout                           as query_timeout_ldap
        , skip_certificate_verification
        , sync_interval_minutes

        -- Ignored - Always null
        -- , channel

        -- Metadata from Rudderstack
        , context_library_name
        , context_library_version
        , sent_at
        , original_timestamp

        -- Ignored -- Always same value
        -- , context_destination_id
        -- , context_destination_type
        -- , context_source_id
        -- , context_source_type
        
        -- Ignored - used by Rudderstack for debugging purposes
        -- , uuid_ts

    from source
)

select * from renamed
