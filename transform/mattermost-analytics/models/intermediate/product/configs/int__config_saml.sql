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

    , enable_saml 
    , enable_admin_attribute
    , enable_sync_with_ldap
    , enable_sync_with_ldap_include_auth
    , encrypt_saml
    , null as ignore_guests_ldap_sync
    , isdefault_admin_attribute
    , isdefault_canonical_algorithm
    , isdefault_email_attribute_saml
    , isdefault_first_name_attribute_saml
    , isdefault_guest_attribute
    , isdefault_id_attribute_saml
    , isdefault_last_name_attribute_saml
    , isdefault_locale_attribute
    , isdefault_login_button_border_color_saml
    , isdefault_login_button_color_saml
    , isdefault_login_button_text
    , isdefault_login_button_text_color_saml
    , isdefault_nickname_attribute_saml
    , isdefault_position_attribute_saml
    , isdefault_scoping_idp_name
    , isdefault_scoping_idp_provider_id
    , isdefault_signature_algorithm
    , isdefault_username_attribute_saml
    , sign_request
    , verify_saml

    , context_library_name
    , context_library_version
    , sent_at
    , original_timestamp
from {{ ref('int_mattermost2__config_saml') }}
union
select event_id
    , event_table
    , event_name
    , server_id
    , received_at
    , timestamp

     -- Server info
    , installation_id
    , anonymous_id
    , server_ip

    , enable_saml 
    , enable_admin_attribute
    , enable_sync_with_ldap
    , enable_sync_with_ldap_include_auth
    , encrypt_saml
    , ignore_guests_ldap_sync
    , isdefault_admin_attribute
    , isdefault_canonical_algorithm
    , isdefault_email_attribute_saml
    , isdefault_first_name_attribute_saml
    , isdefault_guest_attribute
    , isdefault_id_attribute_saml
    , isdefault_last_name_attribute_saml
    , isdefault_locale_attribute
    , isdefault_login_button_border_color_saml
    , isdefault_login_button_color_saml
    , isdefault_login_button_text
    , isdefault_login_button_text_color_saml
    , isdefault_nickname_attribute_saml
    , isdefault_position_attribute_saml
    , isdefault_scoping_idp_name
    , isdefault_scoping_idp_provider_id
    , isdefault_signature_algorithm
    , isdefault_username_attribute_saml
    , sign_request
    , verify_saml

    , context_library_name
    , context_library_version
    , sent_at
    , original_timestamp
from {{ ref('int_mm_telemetry_prod__config_saml') }}

