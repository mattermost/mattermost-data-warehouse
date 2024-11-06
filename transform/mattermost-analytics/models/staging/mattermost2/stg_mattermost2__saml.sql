with source as (
    select * from {{ source('mattermost2', 'config_saml') }}
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

        , enable                                  as enable_saml 
        , enable_admin_attribute
        , enable_sync_with_ldap
        , enable_sync_with_ldap_include_auth
        , encrypt                                 as encrypt_saml
        , isdefault_admin_attribute
        , isdefault_canonical_algorithm
        , isdefault_email_attribute               as isdefault_email_attribute_saml
        , isdefault_first_name_attribute          as isdefault_first_name_attribute_saml
        , isdefault_guest_attribute
        , isdefault_id_attribute                  as isdefault_id_attribute_saml
        , isdefault_last_name_attribute           as isdefault_last_name_attribute_saml
        , isdefault_locale_attribute
        , isdefault_login_button_border_color     as isdefault_login_button_border_color_saml
        , isdefault_login_button_color            as isdefault_login_button_color_saml
        , isdefault_login_button_text
        , isdefault_login_button_text_color       as isdefault_login_button_text_color_saml
        , isdefault_nickname_attribute            as isdefault_nickname_attribute_saml
        , isdefault_position_attribute            as isdefault_position_attribute_saml
        , isdefault_scoping_idp_name
        , isdefault_scoping_idp_provider_id
        , isdefault_signature_algorithm
        , isdefault_username_attribute            as isdefault_username_attribute_saml
        , sign_request
        , verify                                  as verify_saml

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
