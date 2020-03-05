{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp            AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_analytics') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_analytics_details AS (
         SELECT
             timestamp::DATE                         AS date
           , ca.user_id                              AS server_id
           , MAX(isdefault_max_users_for_statistics) AS isdefault_max_users_for_statistics
         FROM {{ source('mattermost2', 'config_analytics') }} ca
              JOIN max_timestamp           mt
                   ON ca.user_id = mt.user_id
                       AND mt.max_timestamp = ca.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_analytics_details