{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp              AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
      , COUNT(user_id)  AS occurrences
    FROM {{ source('mattermost2', 'permissions_general') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::DATE > (SELECT MAX(timestamp::date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_permissions_general_details AS (
         SELECT
             p.timestamp::DATE               AS date
           , p.user_id
           , MAX(phase_1_migration_complete) AS phase_1_migration_complete
           , MAX(phase_2_migration_complete) AS phase_2_migration_complete
         FROM {{ source('mattermost2', 'permissions_general') }} p
              JOIN max_timestamp                  mt
                   ON p.user_id = mt.user_id
                       AND p.timestamp = mt.max_timestamp
         GROUP BY 1, 2)
SELECT *
FROM server_permissions_general_details;