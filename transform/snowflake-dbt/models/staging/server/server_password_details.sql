{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_password') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_password_details AS (
         SELECT
             timestamp::DATE                             AS date
           , p.user_id                                   AS server_id
           , MAX(lowercase)                              AS enable_lowercase
           , MAX(uppercase)                              AS enable_uppercase
           , MAX(symbol)                                 AS enable_symbol
           , MAX(number)                                 AS enable_number
           , MAX(minimum_length)                         AS password_minimum_length
           , {{ dbt_utils.surrogate_key('timestamp::date', 'p.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_password') }} p
              JOIN max_timestamp          mt
                   ON p.user_id = mt.user_id
                       AND mt.max_timestamp = p.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_password_details