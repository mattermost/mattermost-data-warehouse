{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , max(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_email') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_email_details AS (
         SELECT
             timestamp::DATE                           AS date
           , e.user_id                                 AS server_id
           , max(isdefault_feedback_name)              AS isdefault_feedback_name
           , max(isdefault_feedback_organization)      AS isdefault_feedback_organization
           , max(skip_server_certificate_verification) AS skip_server_certificate_verification
           , max(email_batching_buffer_size)           AS email_batching_buffer_size
           , max(enable_smtp_auth)                     AS enable_smtp_auth
           , max(send_email_notifications)             AS send_email_notifications
           , max(isdefault_login_button_color)         AS isdefault_login_button_color
           , max(isdefault_login_button_border_color)  AS isdefault_login_button_border_color
           , max(isdefault_login_button_text_color)    AS isdefault_login_button_text_color
           , max(send_push_notifications)              AS send_push_notifications
           , max(use_channel_in_email_notifications)   AS use_channel_in_email_notifications
           , max(connection_security)                  AS connection_security
           , max(enable_sign_in_with_username)         AS enable_sign_in_with_username
           , max(isdefault_feedback_email)             AS isdefault_feedback_email
           , max(push_notification_contents)           AS push_notification_contents
           , max(enable_sign_in_with_email)            AS enable_sign_in_with_email
           , max(enable_sign_up_with_email)            AS enable_sign_up_with_email
           , max(require_email_verification)           AS require_email_verification
           , max(isdefault_reply_to_address)           AS isdefault_reply_to_address
           , max(email_batching_interval)              AS email_batching_interval
           , max(email_batching_buffer_size)           AS email_batching_buffer_size
           , max(email_notification_contents_type)     AS email_notification_contents_type
           , max(enable_email_batching)                AS enable_email_batching
           , max(enable_preview_mode_banner)           AS enable_preview_mode_banner
         FROM {{ source('staging_config', 'config_email') }} e
              JOIN max_timestamp       mt
                   ON e.user_id = mt.user_id
                       AND mt.max_timestamp = e.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_email_details