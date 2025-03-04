with source as (
    select * from {{ source('mattermost2', 'config_ldap') }}
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
        , max_page_size
        , query_timeout                           as query_timeout_ldap
        , segment_dedupe_id                       as segment_dedupe_id_ldap
        , skip_certificate_verification
        , sync_interval_minutes

        -- Metadata from Segment
        , context_library_name
        , context_library_version
        , sent_at
        , try_to_timestamp_ntz(original_timestamp) as original_timestamp

        -- Ignored - used by segment for debugging purposes
        -- , uuid_ts

    from source
)

select * from renamed
