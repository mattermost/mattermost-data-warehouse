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
    FROM {{ source('mattermost2', 'config_file') }}
    WHERE timestamp::DATE <= CURRENT_DATE - INTERVAL '1 DAY'
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_file_details AS (
         SELECT
             timestamp::DATE              AS date
           , f.user_id                    AS server_id
           , MAX(amazon_s3_signv2)        AS amazon_s3_signv2
           , MAX(amazon_s3_sse)           AS amazon_s3_sse
           , MAX(amazon_s3_ssl)           AS amazon_s3_ssl
           , MAX(amazon_s3_trace)         AS amazon_s3_trace
           , MAX(driver_name)             AS driver_name
           , MAX(enable_file_attachments) AS enable_file_attachments
           , MAX(enable_mobile_download)  AS enable_mobile_download
           , MAX(enable_mobile_upload)    AS enable_mobile_upload
           , MAX(enable_public_links)     AS enable_public_links
           , MAX(isabsolute_directory)    AS isabsolute_directory
           , MAX(isdefault_directory)     AS isdefault_directory
           , MAX(max_file_size)           AS max_file_size
           , MAX(preview_height)          AS preview_height
           , MAX(preview_width)           AS preview_width
           , MAX(profile_height)          AS profile_height
           , MAX(profile_width)           AS profile_width
           , MAX(thumbnail_height)        AS thumbnail_height
           , MAX(thumbnail_width)         AS thumbnail_width
         FROM {{ source('mattermost2', 'config_file') }} f
              JOIN max_timestamp      mt
                   ON f.user_id = mt.user_id
                       AND mt.max_timestamp = f.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_file_details