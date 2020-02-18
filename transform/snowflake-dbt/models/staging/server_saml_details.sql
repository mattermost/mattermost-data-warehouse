{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_saml') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_saml_details AS (
         SELECT
             timestamp::DATE                          AS date
           , s.user_id                                AS server_id
           , MAX(enable)                              AS enable_saml
           , MAX(enable_admin_attribute)              AS enable_admin_attribute
           , MAX(enable_sync_with_ldap)               AS enable_sync_with_ldap
           , MAX(enable_sync_with_ldap_include_auth)  AS enable_sync_with_ldap_include_auth
           , MAX(encrypt)                             AS encrypt_saml
           , MAX(isdefault_admin_attribute)           AS isdefault_admin_attribute
           , MAX(isdefault_canonical_algorithm)       AS isdefault_canonical_algorithm
           , MAX(isdefault_email_attribute)           AS isdefault_email_attribute
           , MAX(isdefault_first_name_attribute)      AS isdefault_first_name_attribute
           , MAX(isdefault_guest_attribute)           AS isdefault_guest_attribute
           , MAX(isdefault_id_attribute)              AS isdefault_id_attribute
           , MAX(isdefault_last_name_attribute)       AS isdefault_last_name_attribute
           , MAX(isdefault_locale_attribute)          AS isdefault_locale_attribute
           , MAX(isdefault_login_button_border_color) AS isdefault_login_button_border_color
           , MAX(isdefault_login_button_color)        AS isdefault_login_button_color
           , MAX(isdefault_login_button_text)         AS isdefault_login_button_text
           , MAX(isdefault_login_button_text_color)   AS isdefault_login_button_text_color
           , MAX(isdefault_nickname_attribute)        AS isdefault_nickname_attribute
           , MAX(isdefault_position_attribute)        AS isdefault_position_attribute
           , MAX(isdefault_scoping_idp_name)          AS isdefault_scoping_idp_name
           , MAX(isdefault_scoping_idp_provider_id)   AS isdefault_scoping_idp_provider_id
           , MAX(isdefault_signature_algorithm)       AS isdefault_signature_algorithm
           , MAX(isdefault_username_attribute)        AS isdefault_username_attribute
           , MAX(sign_request)                        AS sign_request
           , MAX(verify)                              AS verify_saml
         FROM {{ source('staging_config', 'config_saml') }} s
              JOIN max_timestamp      mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_saml_details