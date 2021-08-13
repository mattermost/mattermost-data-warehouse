{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_file') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_file') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

     server_file_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::date)              AS date
           , COALESCE(s.user_id, r.user_id)                              AS server_id
           , MAX(COALESCE(s.amazon_s3_signv2, r.amazon_s3_signv2, r.amazon_s_3_signv_2))        AS amazon_s3_signv2
           , MAX(COALESCE(s.amazon_s3_sse, r.amazon_s3_sse, r.amazon_s_3_sse))           AS amazon_s3_sse
           , MAX(COALESCE(s.amazon_s3_ssl, r.amazon_s3_ssl, r.amazon_s_3_ssl))           AS amazon_s3_ssl
           , MAX(COALESCE(s.amazon_s3_trace, r.amazon_s3_trace, r.amazon_s_3_trace))         AS amazon_s3_trace
           , MAX(COALESCE(s.driver_name, r.driver_name))             AS driver_name
           , MAX(COALESCE(s.enable_file_attachments, r.enable_file_attachments)) AS enable_file_attachments
           , MAX(COALESCE(s.enable_mobile_download, r.enable_mobile_download))  AS enable_mobile_download
           , MAX(COALESCE(s.enable_mobile_upload, r.enable_mobile_upload))    AS enable_mobile_upload
           , MAX(COALESCE(s.enable_public_links, r.enable_public_links))     AS enable_public_links
           , MAX(COALESCE(s.isabsolute_directory, r.isabsolute_directory))    AS isabsolute_directory
           , MAX(COALESCE(s.isdefault_directory, r.isdefault_directory))     AS isdefault_directory
           , MAX(COALESCE(s.max_file_size, r.max_file_size))           AS max_file_size
           , MAX(COALESCE(s.preview_height, NULL))          AS preview_height
           , MAX(COALESCE(s.preview_width, NULL))           AS preview_width
           , MAX(COALESCE(s.profile_height, NULL))          AS profile_height
           , MAX(COALESCE(s.profile_width, NULL))           AS profile_width
           , MAX(COALESCE(s.thumbnail_height, NULL))        AS thumbnail_height
           , MAX(COALESCE(s.thumbnail_width, NULL))         AS thumbnail_width           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           , MAX(COALESCE(r.EXTRACT_CONTENT,  NULL)) AS extract_content
           , MAX(COALESCE(r.ARCHIVE_RECURSION,  NULL)) AS archive_recursion
           FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_file') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_file') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 21, 22
     )
SELECT *
FROM server_file_details