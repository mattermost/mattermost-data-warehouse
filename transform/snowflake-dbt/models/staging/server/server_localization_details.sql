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
    FROM {{ source('mattermost2', 'config_localization') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_localization_details AS (
         SELECT
             timestamp::DATE            AS date
           , l.user_id                  AS server_id
           , MAX(available_locales)     AS available_locales
           , MAX(default_client_locale) AS default_client_locale
           , MAX(default_server_locale) AS default_server_locale
         FROM {{ source('mattermost2', 'config_localization') }} l
              JOIN max_timestamp      mt
                   ON l.user_id = mt.user_id
                       AND mt.max_timestamp = l.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_localization_details