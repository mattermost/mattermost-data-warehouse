{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_ldap') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_ldap') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_ldap_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::date)              AS date
           , COALESCE(s.user_id, r.user_id)                              AS server_id
           , MAX(COALESCE(s.connection_security, r.connection_security))                    AS connection_security
           , MAX(COALESCE(s.enable, r.enable))                                 AS enable_ldap
           , MAX(COALESCE(s.enable_admin_filter, r.enable_admin_filter))                    AS enable_admin_filter
           , MAX(COALESCE(s.enable_sync, r.enable_sync))                            AS enable_sync
           , MAX(COALESCE(s.isdefault_email_attribute, r.isdefault_email_attribute))              AS isdefault_email_attribute
           , MAX(COALESCE(s.isdefault_first_name_attribute, r.isdefault_first_name_attribute))         AS isdefault_first_name_attribute
           , MAX(COALESCE(s.isdefault_group_display_name_attribute, r.isdefault_group_display_name_attribute)) AS isdefault_group_display_name_attribute
           , MAX(COALESCE(s.isdefault_group_id_attribute, r.isdefault_group_id_attribute))           AS isdefault_group_id_attribute
           , MAX(COALESCE(s.isdefault_id_attribute, r.isdefault_id_attribute))                 AS isdefault_id_attribute
           , MAX(COALESCE(s.isdefault_last_name_attribute, r.isdefault_last_name_attribute))          AS isdefault_last_name_attribute
           , MAX(COALESCE(s.isdefault_login_button_border_color, r.isdefault_login_button_border_color))    AS isdefault_login_button_border_color
           , MAX(COALESCE(s.isdefault_login_button_color, r.isdefault_login_button_color))           AS isdefault_login_button_color
           , MAX(COALESCE(s.isdefault_login_button_text_color, r.isdefault_login_button_text_color))      AS isdefault_login_button_text_color
           , MAX(COALESCE(s.isdefault_login_field_name, r.isdefault_login_field_name))             AS isdefault_login_field_name
           , MAX(COALESCE(s.isdefault_login_id_attribute, r.isdefault_login_id_attribute))           AS isdefault_login_id_attribute
           , MAX(COALESCE(s.isdefault_nickname_attribute, r.isdefault_nickname_attribute))           AS isdefault_nickname_attribute
           , MAX(COALESCE(s.isdefault_position_attribute, r.isdefault_position_attribute))           AS isdefault_position_attribute
           , MAX(COALESCE(s.isdefault_username_attribute, r.isdefault_username_attribute))           AS isdefault_username_attribute
           , MAX(COALESCE(s.isempty_admin_filter, r.isempty_admin_filter))                   AS isempty_admin_filter
           , MAX(COALESCE(s.isempty_group_filter, r.isempty_group_filter))                   AS isempty_group_filter
           , MAX(COALESCE(s.isempty_guest_filter, r.isempty_guest_filter))                   AS isempty_guest_filter
           , MAX(COALESCE(s.max_page_size, r.max_page_size))                          AS max_page_size
           , MAX(COALESCE(s.query_timeout, r.query_timeout))                          AS query_timeout
           , MAX(COALESCE(s.segment_dedupe_id, NULL))                      AS segment_dedupe_id
           , MAX(COALESCE(s.skip_certificate_verification, r.skip_certificate_verification))          AS skip_certificate_verification
           , MAX(COALESCE(s.sync_interval_minutes, r.sync_interval_minutes))                  AS sync_interval_minutes           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           , MAX(COALESCE(r.isnotempty_private_key, NULL))        AS isnotempty_private_key
           , MAX(COALESCE(r.isnotempty_public_certificate, NULL))        AS isnotempty_public_certificate
           FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_ldap') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_ldap') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 29, 30
     )
SELECT *
FROM server_ldap_details