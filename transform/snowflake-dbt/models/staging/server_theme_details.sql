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
    FROM {{ source('mattermost2', 'config_theme') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_theme_details AS (
         SELECT
             timestamp::DATE              AS date
           , t.user_id                    AS server_id
           , MAX(allowed_themes)          AS allowed_themes
           , MAX(allow_custom_themes)     AS allow_custom_themes
           , MAX(enable_theme_selection)  AS enable_theme_selection
           , MAX(isdefault_default_theme) AS isdefault_default_theme
         FROM {{ source('mattermost2', 'config_theme') }} t
              JOIN max_timestamp       mt
                   ON t.user_id = mt.user_id
                       AND mt.max_timestamp = t.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_theme_details