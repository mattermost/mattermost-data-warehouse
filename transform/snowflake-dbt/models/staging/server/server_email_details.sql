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
    FROM {{ source('mattermost2', 'config_email') }}
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
    FROM {{ source('mm_telemetry_prod', 'config_email') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

     server_email_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::date)              AS date
           , COALESCE(s.user_id, r.user_id)                              AS server_id
           , MAX(COALESCE(s.connection_security, r.connection_security))                  AS connection_security
           , MAX(COALESCE(s.email_batching_buffer_size, r.email_batching_buffer_size))           AS email_batching_buffer_size
           , MAX(COALESCE(s.email_batching_interval, r.email_batching_interval))              AS email_batching_interval
           , MAX(COALESCE(s.email_notification_contents_type, r.email_notification_contents_type))     AS email_notification_contents_type
           , MAX(COALESCE(s.enable_email_batching, r.enable_email_batching))                AS enable_email_batching
           , MAX(COALESCE(s.enable_preview_mode_banner, r.enable_preview_mode_banner))           AS enable_preview_mode_banner
           , MAX(COALESCE(s.enable_sign_in_with_email, r.enable_sign_in_with_email))            AS enable_sign_in_with_email
           , MAX(COALESCE(s.enable_sign_in_with_username, r.enable_sign_in_with_username))         AS enable_sign_in_with_username
           , MAX(COALESCE(s.enable_sign_up_with_email, r.enable_sign_up_with_email))            AS enable_sign_up_with_email
           , MAX(COALESCE(s.enable_smtp_auth, r.enable_smtp_auth))                     AS enable_smtp_auth
           , MAX(COALESCE(s.isdefault_feedback_email, r.isdefault_feedback_email))             AS isdefault_feedback_email
           , MAX(COALESCE(s.isdefault_feedback_name, r.isdefault_feedback_name))              AS isdefault_feedback_name
           , MAX(COALESCE(s.isdefault_feedback_organization, r.isdefault_feedback_organization))      AS isdefault_feedback_organization
           , MAX(COALESCE(s.isdefault_login_button_border_color, r.isdefault_login_button_border_color))  AS isdefault_login_button_border_color
           , MAX(COALESCE(s.isdefault_login_button_color, r.isdefault_login_button_color))         AS isdefault_login_button_color
           , MAX(COALESCE(s.isdefault_login_button_text_color, r.isdefault_login_button_text_color))    AS isdefault_login_button_text_color
           , MAX(COALESCE(s.isdefault_reply_to_address, r.isdefault_reply_to_address))           AS isdefault_reply_to_address
           , MAX(COALESCE(s.push_notification_contents, r.push_notification_contents))           AS push_notification_contents
           , MAX(COALESCE(s.require_email_verification, r.require_email_verification))           AS require_email_verification
           , MAX(COALESCE(s.send_email_notifications, r.send_email_notifications))             AS send_email_notifications
           , MAX(COALESCE(s.send_push_notifications, r.send_push_notifications))              AS send_push_notifications
           , MAX(COALESCE(s.skip_server_certificate_verification, r.skip_server_certificate_verification)) AS skip_server_certificate_verification
           , MAX(COALESCE(s.use_channel_in_email_notifications, r.use_channel_in_email_notifications))   AS use_channel_in_email_notifications           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_email') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_email') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 26, 27
     )
SELECT *
FROM server_email_details