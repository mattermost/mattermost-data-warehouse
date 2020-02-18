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
    FROM {{ source('staging_config', 'config_nativeapp') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_nativeapp_details AS (
         SELECT
             timestamp::DATE                          AS date
           , n.user_id                                AS server_id
           , MAX(isdefault_android_app_download_link) AS isdefault_android_app_download_link
           , MAX(isdefault_app_download_link)         AS isdefault_app_download_link
           , MAX(isdefault_iosapp_download_link)      AS isdefault_iosapp_download_link
         FROM {{ source('staging_config', 'config_nativeapp') }} n
              JOIN max_timestamp         mt
                   ON n.user_id = mt.user_id
                       AND mt.max_timestamp = n.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_nativeapp_details