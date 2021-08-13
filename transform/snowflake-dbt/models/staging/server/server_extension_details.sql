{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_timestamp        AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_extension') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),
     server_extension_details AS (
         SELECT
             timestamp::DATE                          AS date
           , e.user_id                                AS server_id
           , MAX(enable_experimental_extensions)      AS enable_experimental_extensions
           , {{ dbt_utils.surrogate_key(['timestamp::date', 'e.user_id']) }} AS id
         FROM {{ source('mattermost2', 'config_extension') }} e
              JOIN max_timestamp              mt
                   ON e.user_id = mt.user_id
                       AND mt.max_timestamp = e.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_extension_details