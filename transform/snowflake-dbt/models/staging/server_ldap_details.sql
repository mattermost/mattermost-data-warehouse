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
           , MAX(connection_security)                    AS connection_security
           , MAX(enable)                                 AS enable_ldap
           , MAX(enable_admin_filter)                    AS enable_admin_filter
           , MAX(enable_sync)                            AS enable_sync
           , MAX(isdefault_email_attribute)              AS isdefault_email_attribute
           , MAX(isdefault_first_name_attribute)         AS isdefault_first_name_attribute
           , MAX(isdefault_group_display_name_attribute) AS isdefault_group_display_name_attribute
           , MAX(isdefault_group_id_attribute)           AS isdefault_group_id_attribute
           , MAX(isdefault_id_attribute)                 AS isdefault_id_attribute
           , MAX(isdefault_last_name_attribute)          AS isdefault_last_name_attribute
           , MAX(isdefault_login_button_border_color)    AS isdefault_login_button_border_color
           , MAX(isdefault_login_button_color)           AS isdefault_login_button_color
           , MAX(isdefault_login_button_text_color)      AS isdefault_login_button_text_color
           , MAX(isdefault_login_field_name)             AS isdefault_login_field_name
           , MAX(isdefault_login_id_attribute)           AS isdefault_login_id_attribute
           , MAX(isdefault_nickname_attribute)           AS isdefault_nickname_attribute
           , MAX(isdefault_position_attribute)           AS isdefault_position_attribute
           , MAX(isdefault_username_attribute)           AS isdefault_username_attribute
           , MAX(isempty_admin_filter)                   AS isempty_admin_filter
           , MAX(isempty_group_filter)                   AS isempty_group_filter
           , MAX(isempty_guest_filter)                   AS isempty_guest_filter
           , MAX(max_page_size)                          AS max_page_size
           , MAX(query_timeout)                          AS query_timeout
           , MAX(segment_dedupe_id)                      AS segment_dedupe_id
           , MAX(skip_certificate_verification)          AS skip_certificate_verification
           , MAX(sync_interval_minutes)                  AS sync_interval_minutes
         FROM {{ source('staging_config', 'config_ldap') }} l
              JOIN max_timestamp      mt
                   ON l.user_id = mt.user_id
                       AND mt.max_timestamp = l.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_ldap_details