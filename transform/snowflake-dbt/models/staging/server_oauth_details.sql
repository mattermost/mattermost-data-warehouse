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
    FROM {{ source('staging_config', 'config_oauth') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_oauth_details AS (
         SELECT
             timestamp::DATE                             AS date
           , o.user_id                                   AS server_id
           , max(enable_office365)                       AS enable_office365_oauth
           , max(enable_google)                          AS enable_google_oauth
           , max(enable_gitlab)                          AS enable_gitlab_oauth
         FROM {{ source('staging_config', 'config_oauth') }} o
              JOIN max_timestamp                   mt
                   ON o.user_id = mt.user_id
                       AND mt.max_timestamp = o.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_oauth_details