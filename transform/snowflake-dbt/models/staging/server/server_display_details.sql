{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_timestamp                 AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_display') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_display_details AS (
         SELECT
             timestamp::DATE                    AS date
           , d.user_id                          AS server_id
           , MAX(experimental_timezone)         AS experimental_timezone
           , MAX(isdefault_custom_url_schemes)  AS isdefault_custom_url_schemes
           , {{ dbt_utils.surrogate_key('timestamp::date', 'd.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_display') }} d
              JOIN max_timestamp                mt
                   ON d.user_id = mt.user_id
                       AND mt.max_timestamp = d.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_display_details