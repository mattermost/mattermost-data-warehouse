{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_email') }}
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
           , MAX(connection_security)                  AS connection_security
           , MAX(email_batching_buffer_size)           AS email_batching_buffer_size
           , MAX(email_batching_interval)              AS email_batching_interval
           , MAX(email_notification_contents_type)     AS email_notification_contents_type
           , MAX(enable_email_batching)                AS enable_email_batching
           , MAX(enable_preview_mode_banner)           AS enable_preview_mode_banner
           , MAX(enable_sign_in_with_email)            AS enable_sign_in_with_email
           , MAX(enable_sign_in_with_username)         AS enable_sign_in_with_username
           , MAX(enable_sign_up_with_email)            AS enable_sign_up_with_email
           , MAX(enable_smtp_auth)                     AS enable_smtp_auth
           , MAX(isdefault_feedback_email)             AS isdefault_feedback_email
           , MAX(isdefault_feedback_name)              AS isdefault_feedback_name
           , MAX(isdefault_feedback_organization)      AS isdefault_feedback_organization
           , MAX(isdefault_login_button_border_color)  AS isdefault_login_button_border_color
           , MAX(isdefault_login_button_color)         AS isdefault_login_button_color
           , MAX(isdefault_login_button_text_color)    AS isdefault_login_button_text_color
           , MAX(isdefault_reply_to_address)           AS isdefault_reply_to_address
           , MAX(push_notification_contents)           AS push_notification_contents
           , MAX(require_email_verification)           AS require_email_verification
           , MAX(send_email_notifications)             AS send_email_notifications
           , MAX(send_push_notifications)              AS send_push_notifications
           , MAX(skip_server_certificate_verification) AS skip_server_certificate_verification
           , MAX(use_channel_in_email_notifications)   AS use_channel_in_email_notifications
         FROM {{ source('mattermost2', 'config_email') }} e
              JOIN max_timestamp       mt
                   ON e.user_id = mt.user_id
                       AND mt.max_timestamp = e.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_email_details