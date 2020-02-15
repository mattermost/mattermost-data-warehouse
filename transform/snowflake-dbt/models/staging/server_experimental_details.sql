{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp        AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , max(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_experimental') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_experimental_details AS (
         SELECT
             timestamp::DATE                          AS date
           , e.user_id                                AS server_id
           , max(enable_click_to_reply)               AS enable_click_to_reply
           , max(client_side_cert_enable)             AS client_side_cert_enable
           , max(enable_post_metadata)                AS enable_post_metadata
           , max(restrict_system_admin)               AS restrict_system_admin
           , max(isdefault_client_side_cert_check)    AS isdefault_client_side_cert_check
           , max(link_metadata_timeout_milliseconds)  AS link_metadata_timeout_milliseconds
         FROM {{ source('staging_config', 'config_experimental') }} e
              JOIN max_timestamp              mt
                   ON e.user_id = mt.user_id
                       AND mt.max_timestamp = e.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_experimental_details