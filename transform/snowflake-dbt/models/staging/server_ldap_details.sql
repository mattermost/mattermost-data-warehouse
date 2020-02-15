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
    FROM {{ source('staging_config', 'config_ldap') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_ldap_details AS (
         SELECT
             timestamp::DATE                             AS date
           , l.user_id                                   AS server_id
           , max(enable)                                 AS enable_ldap
           , max(isdefault_login_id_attribute)           AS isdefault_login_id_attribute
           , max(isdefault_login_field_name)             AS isdefault_login_field_name
           , max(isdefault_login_button_text_color)      AS isdefault_login_button_text_color
           , max(isdefault_login_button_border_color)    AS isdefault_login_button_border_color
           , max(isdefault_login_button_color)           AS isdefault_login_button_color
           , max(isdefault_id_attribute)                 AS isdefault_id_attribute
           , max(isdefault_first_name_attribute)         AS isdefault_first_name_attribute
           , max(isdefault_last_name_attribute)          AS isdefault_last_name_attribute
           , max(isdefault_nickname_attribute)           AS isdefault_nickname_attribute
           , max(isdefault_username_attribute)           AS isdefault_username_attribute
           , max(isdefault_email_attribute)              AS isdefault_email_attribute
           , max(isdefault_group_id_attribute)           AS isdefault_group_id_attribute
           , max(isdefault_group_display_name_attribute) AS isdefault_group_display_name_attribute
           , max(isempty_group_filter)                   AS isempty_group_filter
           , max(sync_interval_minutes)                  AS sync_interval_minutes
           , max(connection_security)                    AS connection_security
           , max(isdefault_position_attribute)           AS isdefault_position_attribute
           , max(enable_sync)                            AS enable_ldap_sync
           , max(query_timeout)                          AS query_timeout
           , max(max_page_size)                          AS max_page_size
           , max(skip_certificate_verification)          AS skip_certificate_verification
           , max(isempty_guest_filter)                   AS isempty_guest_filter
         FROM {{ source('staging_config', 'config_ldap') }} l
              JOIN max_timestamp      mt
                   ON l.user_id = mt.user_id
                       AND mt.max_timestamp = l.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_ldap_details