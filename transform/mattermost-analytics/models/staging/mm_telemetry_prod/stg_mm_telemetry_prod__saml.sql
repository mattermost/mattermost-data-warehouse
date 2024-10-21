with source as (
    select * from {{ source('mm_telemetry_prod', 'config_saml') }}
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

        , enable                                  as enable_saml 
        , enable_admin_attribute
        , enable_sync_with_ldap
        , enable_sync_with_ldap_include_auth
        , encrypt                                 as encrypt_saml
        , ignore_guests_ldap_sync
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
