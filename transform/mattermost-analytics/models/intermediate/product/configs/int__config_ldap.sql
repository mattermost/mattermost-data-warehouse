{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_xs"
    })
}}

select event_id
    , event_table
    , event_name
    , server_id
    , received_at
    , timestamp

    , null as installation_id
    , null as anonymous_id
    , null as server_ip
    
    , connection_security_ldap
    , enable_ldap
    , enable_admin_filter
    , enable_sync
    , isdefault_email_attribute_ldap
    , isdefault_first_name_attribute_ldap
    , isdefault_group_display_name_attribute
    , isdefault_group_id_attribute
    , isdefault_id_attribute_ldap
    , isdefault_last_name_attribute_ldap
    , isdefault_login_button_border_color_ldap
    , isdefault_login_button_color_ldap
    , isdefault_login_button_text_color_ldap
    , isdefault_login_field_name
    , isdefault_login_id_attribute
    , isdefault_nickname_attribute_ldap
    , isdefault_position_attribute_ldap
    , isdefault_username_attribute_ldap
    , isempty_admin_filter
    , isempty_group_filter
    , isempty_guest_filter
    , isnotempty_picture_attribute
    , null as isnotempty_private_key
    , null as isnotempty_public_certificate
    , max_page_size
    , query_timeout_ldap
    , segment_dedupe_id_ldap
    , skip_certificate_verification
    , sync_interval_minutes

    , context_library_name
    , context_library_version
    , sent_at
    , original_timestamp
from {{ ref('int_mattermost2__config_ldap') }}
union
select event_id
    , event_table
    , event_name
    , server_id
    , received_at
    , timestamp

    , installation_id
    , anonymous_id
    , server_ip

    , connection_security_ldap
    , enable_ldap
    , enable_admin_filter
    , enable_sync
    , isdefault_email_attribute_ldap
    , isdefault_first_name_attribute_ldap
    , isdefault_group_display_name_attribute
    , isdefault_group_id_attribute
    , isdefault_id_attribute_ldap
    , isdefault_last_name_attribute_ldap
    , isdefault_login_button_border_color_ldap
    , isdefault_login_button_color_ldap
    , isdefault_login_button_text_color_ldap
    , isdefault_login_field_name
    , isdefault_login_id_attribute
    , isdefault_nickname_attribute_ldap
    , isdefault_position_attribute_ldap
    , isdefault_username_attribute_ldap
    , isempty_admin_filter
    , isempty_group_filter
    , isempty_guest_filter
    , isnotempty_picture_attribute
    , isnotempty_private_key
    , isnotempty_public_certificate
    , max_page_size
    , query_timeout_ldap
    , null as segment_dedupe_id_ldap
    , skip_certificate_verification
    , sync_interval_minutes

    , context_library_name
    , context_library_version
    , sent_at
    , original_timestamp
from {{ ref('int_mm_telemetry_prod__config_ldap') }}

