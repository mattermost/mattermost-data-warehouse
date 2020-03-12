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
    FROM {{ source('mattermost2', 'config_oauth') }}
    WHERE timestamp::DATE <= CURRENT_DATE - INTERVAL '1 DAY'
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_oauth_details AS (
         SELECT
             timestamp::DATE                             AS date
           , o.user_id                                   AS server_id
           , MAX(enable_office365)                       AS enable_office365_oauth
           , MAX(enable_google)                          AS enable_google_oauth
           , MAX(enable_gitlab)                          AS enable_gitlab_oauth
         FROM {{ source('mattermost2', 'config_oauth') }} o
              JOIN max_timestamp                   mt
                   ON o.user_id = mt.user_id
                       AND mt.max_timestamp = o.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_oauth_details