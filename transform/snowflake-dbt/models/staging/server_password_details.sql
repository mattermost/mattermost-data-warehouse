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
    FROM {{ source('staging_config', 'config_password') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_password_details AS (
         SELECT
             timestamp::DATE                             AS date
           , p.user_id                                   AS server_id
           , max(lowercase)                              AS password_lowercase
           , max(uppercase)                              AS password_uppercase
           , max(symbol)                                 AS password_symbol
           , max(number)                                 AS password_number
           , max(minimum_length)                         AS password_minimum_length
         FROM {{ source('staging_config', 'config_password') }} p
              JOIN max_timestamp          mt
                   ON p.user_id = mt.user_id
                       AND mt.max_timestamp = p.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_password_details