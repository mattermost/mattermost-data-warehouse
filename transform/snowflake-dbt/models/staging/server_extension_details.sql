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
    FROM {{ source('staging_config', 'config_extension') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_extension_details AS (
         SELECT
             timestamp::DATE                          AS date
           , e.user_id                                AS server_id
           , max(enable_experimental_extensions)      AS enable_experimental_extensions
         FROM {{ source('staging_config', 'config_extension') }} e
              JOIN max_timestamp              mt
                   ON e.user_id = mt.user_id
                       AND mt.max_timestamp = e.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_extension_details