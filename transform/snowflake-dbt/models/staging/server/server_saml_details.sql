{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp        AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_saml') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_saml') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_saml_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , MAX(COALESCE(r.enable, s.enable))                              AS enable_saml
           , MAX(COALESCE(r.enable_admin_attribute, s.enable_admin_attribute))              AS enable_admin_attribute
           , MAX(COALESCE(r.enable_sync_with_ldap, s.enable_sync_with_ldap))               AS enable_sync_with_ldap
           , MAX(COALESCE(r.enable_sync_with_ldap_include_auth, s.enable_sync_with_ldap_include_auth))  AS enable_sync_with_ldap_include_auth
           , MAX(COALESCE(r.encrypt, s.encrypt))                             AS encrypt_saml
           , MAX(COALESCE(r.isdefault_admin_attribute, s.isdefault_admin_attribute))           AS isdefault_admin_attribute
           , MAX(COALESCE(r.isdefault_canonical_algorithm, s.isdefault_canonical_algorithm))       AS isdefault_canonical_algorithm
           , MAX(COALESCE(r.isdefault_email_attribute, s.isdefault_email_attribute))           AS isdefault_email_attribute
           , MAX(COALESCE(r.isdefault_first_name_attribute, s.isdefault_first_name_attribute))      AS isdefault_first_name_attribute
           , MAX(COALESCE(r.isdefault_guest_attribute, s.isdefault_guest_attribute))           AS isdefault_guest_attribute
           , MAX(COALESCE(r.isdefault_id_attribute, s.isdefault_id_attribute))              AS isdefault_id_attribute
           , MAX(COALESCE(r.isdefault_last_name_attribute, s.isdefault_last_name_attribute))       AS isdefault_last_name_attribute
           , MAX(COALESCE(r.isdefault_locale_attribute, s.isdefault_locale_attribute))          AS isdefault_locale_attribute
           , MAX(COALESCE(r.isdefault_login_button_border_color, s.isdefault_login_button_border_color)) AS isdefault_login_button_border_color
           , MAX(COALESCE(r.isdefault_login_button_color, s.isdefault_login_button_color))        AS isdefault_login_button_color
           , MAX(COALESCE(r.isdefault_login_button_text, s.isdefault_login_button_text))         AS isdefault_login_button_text
           , MAX(COALESCE(r.isdefault_login_button_text_color, s.isdefault_login_button_text_color))   AS isdefault_login_button_text_color
           , MAX(COALESCE(r.isdefault_nickname_attribute, s.isdefault_nickname_attribute))        AS isdefault_nickname_attribute
           , MAX(COALESCE(r.isdefault_position_attribute, s.isdefault_position_attribute))        AS isdefault_position_attribute
           , MAX(COALESCE(r.isdefault_scoping_idp_name, s.isdefault_scoping_idp_name))          AS isdefault_scoping_idp_name
           , MAX(COALESCE(r.isdefault_scoping_idp_provider_id, s.isdefault_scoping_idp_provider_id))   AS isdefault_scoping_idp_provider_id
           , MAX(COALESCE(r.isdefault_signature_algorithm, s.isdefault_signature_algorithm))       AS isdefault_signature_algorithm
           , MAX(COALESCE(r.isdefault_username_attribute, s.isdefault_username_attribute))        AS isdefault_username_attribute
           , MAX(COALESCE(r.sign_request, s.sign_request))                        AS sign_request
           , MAX(COALESCE(r.verify, s.verify))                              AS verify_saml           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           , MAX(COALESCE(r.ignore_guests_ldap_sync, NULL)) AS ignore_guests_ldap_sync
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_saml') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_saml') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 28, 29
     )
SELECT *
FROM server_saml_details