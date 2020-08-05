{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_nativeapp') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

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
           , {{ dbt_utils.surrogate_key('timestamp::date', 'n.user_id') }} AS id
         FROM {{ source('mattermost2', 'config_nativeapp') }} n
              JOIN max_timestamp         mt
                   ON n.user_id = mt.user_id
                       AND mt.max_timestamp = n.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_nativeapp_details