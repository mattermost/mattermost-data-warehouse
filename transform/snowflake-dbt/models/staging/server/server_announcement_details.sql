{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_timestamp               AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_announcement') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_announcement_details AS (
         SELECT
             timestamp::DATE                  AS date
           , ca.user_id                       AS server_id
           , MAX(allow_banner_dismissal)      AS allow_banner_dismissal
           , MAX(enable_banner)               AS enable_banner
           , MAX(isdefault_banner_color)      AS isdefault_banner_color
           , MAX(isdefault_banner_text_color) AS isdefault_banner_text_color
           , {{ dbt_utils.surrogate_key('timestamp::date', 'ca.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_announcement') }} ca
              JOIN max_timestamp              mt
                   ON ca.user_id = mt.user_id
                       AND mt.max_timestamp = ca.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_announcement_details