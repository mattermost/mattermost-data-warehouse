{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp               AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , max(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_announcement') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_announcement_details AS (
         SELECT
             timestamp::DATE                  AS date
           , ca.user_id                       AS server_id
           , max(enable_banner)               AS enabled_banner
           , max(isdefault_banner_color)      AS isdefault_banner_color
           , max(isdefault_banner_text_color) AS isdefault_banner_text_color
           , max(allow_banner_dismissal)      AS allow_banner_dismissal
         FROM {{ source('staging_config', 'config_announcement') }} ca
              JOIN max_timestamp              mt
                   ON ca.user_id = mt.user_id
                       AND mt.max_timestamp = ca.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_announcement_details