{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_image_proxy') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_image_proxy_details AS (
         SELECT
             timestamp::DATE                           AS date
           , i.user_id                                 AS server_id
           , MAX(enable)                               AS enable_image_proxy
           , MAX(image_proxy_type)                     AS image_proxy_type
           , MAX(isdefault_remote_image_proxy_options) AS isdefault_remote_image_proxy_options
           , MAX(isdefault_remote_image_proxy_url)     AS isdefault_remote_image_proxy_url
           , {{ dbt_utils.surrogate_key('timestamp::date', 'i.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_image_proxy') }} i
              JOIN max_timestamp             mt
                   ON i.user_id = mt.user_id
                       AND mt.max_timestamp = i.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_image_proxy_details