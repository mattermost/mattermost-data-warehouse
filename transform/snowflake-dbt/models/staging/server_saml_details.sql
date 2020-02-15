{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , max(timestamp)  AS max_timestamp
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
           , max(isdefault_login_button_color)        AS isdefault_login_button_color
           , max(isdefault_login_button_border_color) AS isdefault_login_button_border_color
           , max(isdefault_login_button_text)         AS isdefault_login_button_text
           , max(isdefault_login_button_text_color)   AS isdefault_login_button_text_color
           , max(isdefault_id_attribute)              AS isdefault_id_attribute
           , max(isdefault_first_name_attribute)      AS isdefault_first_name_attribute
           , max(isdefault_last_name_attribute)       AS isdefault_last_name_attribute
           , max(isdefault_nickname_attribute)        AS isdefault_nickname_attribute
           , max(isdefault_email_attribute)           AS isdefault_email_attribute
           , max(isdefault_position_attribute)        AS isdefault_position_attribute
           , max(isdefault_locale_attribute)          AS isdefault_locale_attribute
           , max(isdefault_guest_attribute)           AS isdefault_guest_attribute
           , max(enable)                              AS enable_saml
           , max(enable_sync_with_ldap)               AS enable_sync_with_ldap
           , max(enable_sync_with_ldap_include_auth)  AS enable_sync_with_ldap_include_auth
           , max(encrypt)                             AS encrypt
           , max(verify)                              AS verify
           , max(sign_request)                        AS sign_request
           , max(isdefault_canonical_algorithm)       AS isdefault_canonical_algorithm
           , max(isdefault_signature_algorithm)       AS isdefault_signature_algorithm
           , max(isdefault_scoping_idp_provider_id)   AS isdefault_scoping_idp_provider_id
           , max(isdefault_scoping_idp_name)          AS isdefault_scoping_idp_name
         FROM {{ source('staging_config', 'config_saml') }} s
              JOIN max_timestamp      mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_saml_details