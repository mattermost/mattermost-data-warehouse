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
    FROM {{ source('staging_config', 'config_message_export') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_message_export_details AS (
         SELECT
             timestamp::DATE                            AS date
           , m.user_id                                  AS server_id
           , MAX(batch_size)                            AS batch_size
           , MAX(daily_run_time)                        AS daily_run_time
           , MAX(enable_message_export)                 AS enable_message_export
           , MAX(export_format)                         AS export_format
           , MAX(global_relay_customer_type)            AS global_relay_customer_type
           , MAX(is_default_global_relay_email_address) AS is_default_global_relay_email_address
           , MAX(is_default_global_relay_smtp_password) AS is_default_global_relay_smtp_password
           , MAX(is_default_global_relay_smtp_username) AS is_default_global_relay_smtp_username
         FROM {{ source('staging_config', 'config_message_export') }} m
              JOIN max_timestamp                mt
                   ON m.user_id = mt.user_id
                       AND mt.max_timestamp = m.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_message_export_details