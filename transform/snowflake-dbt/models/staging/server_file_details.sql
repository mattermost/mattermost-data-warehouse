{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , max(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_file') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_file_details AS (
         SELECT
             timestamp::DATE                       AS date
           , f.user_id                             AS server_id
           , max(driver_name)                      AS driver_name
           , max(amazon_s3_trace)                  AS amazon_s3_trace
           , max(amazon_s3_signv2)                 AS amazon_s3_signv2
           , max(amazon_s3_sse)                    AS amazon_s3_sse
           , max(amazon_s3_ssl)                    AS amazon_s3_ssl
           , max(enable_public_links)              AS enable_public_links
           , max(enable_file_attachments)          AS enable_file_attachments
           , max(enable_mobile_download)           AS enable_mobile_download
           , max(enable_mobile_upload)             AS enable_mobile_upload
           , max(isabsolute_directory)             AS isabsolute_directory
           , max(isdefault_directory)              AS isdefault_directory
           , max(max_file_size)                    AS max_file_size
           , max(preview_height)                   AS preview_height
           , max(preview_width)                    AS preview_width
           , max(profile_height)                   AS profile_height
           , max(profile_width)                    AS profile_width
           , max(thumbnail_height)                 AS thumbnail_height
           , max(thumbnail_width)                  AS thumbnail_width
         FROM {{ source('staging_config', 'config_file') }} f
              JOIN max_timestamp      mt
                   ON f.user_id = mt.user_id
                       AND mt.max_timestamp = f.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_file_details