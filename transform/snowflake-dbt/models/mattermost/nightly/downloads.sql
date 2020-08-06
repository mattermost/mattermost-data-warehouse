{{config({
    "materialized": 'incremental',
    "schema": "mattermost"
  })
}}

WITH download_stats         AS (
    SELECT
        logdate::DATE                                           AS logdate
      , logtime
      , cip
      , uri
      , bytessent
      , abs(bytessent - avg(bytessent) OVER (PARTITION BY uri)) AS diff
      , avg(bytessent) OVER (PARTITION BY uri)                  AS avg
      , max(bytessent) OVER (PARTITION BY uri)                  AS max
      , stddev(bytessent) OVER (PARTITION BY uri) * 1           AS std
      , count(bytessent) OVER (PARTITION BY uri)                AS count
      , CASE
            WHEN abs(bytessent - avg(bytessent) OVER (PARTITION BY uri)) >
                 stddev(bytessent) OVER (PARTITION BY uri) * 1 THEN TRUE
            ELSE FALSE END                                      AS one_std_from_mean
      , MIN((logdate::DATE || ' ' || logtime)::timestamp) 
            OVER (PARTITION BY cip)                             AS first_install_datetime
    FROM {{ source('releases', 'log_entries') }} log_entries
    WHERE (regexp_like(uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') OR
           regexp_like(log_entries.uri, '^\/desktop\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*'))
      AND bytessent > 0
      AND status LIKE '2%'
),

     downloads AS (
         SELECT
             log_entries.logdate::DATE                                                             AS logdate
           , log_entries.logtime
           , log_entries.cip                                                                       AS ip_address
           , log_entries.uri
           , CASE
                 WHEN regexp_like(log_entries.uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') THEN 'server'
                 WHEN regexp_like(log_entries.uri, '^\/desktop\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') THEN 'app'
                 ELSE NULL END                                                                     AS category
           , CASE
                 WHEN regexp_like(log_entries.uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*')
                     THEN CASE
                              WHEN log_entries.uri LIKE '%team%' THEN 'team'
                              ELSE 'enterprise' END
                 WHEN regexp_like(log_entries.uri, '^\/desktop\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*')
                     THEN 'desktop'
                 ELSE NULL END                                                                     AS type
           , CASE
                 WHEN regexp_like(log_entries.uri, '^\/desktop\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*')
                     THEN CASE
                              WHEN log_entries.uri LIKE '%linux%' THEN 'linux'
                              WHEN regexp_like(log_entries.uri,
                                               '.*msi$|.*win[0-9]{2}.*$|.*\-win\-.*$|.*\-win\.[a-z]{3}$') THEN 'windows'
                              WHEN regexp_like(log_entries.uri, '.*mac\.[a-z]{3}$|.*\-osx\..*$') THEN 'mac'
                              ELSE NULL END
                 WHEN regexp_like(log_entries.uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*')
                     THEN CASE
                              WHEN log_entries.uri LIKE '%linux%' THEN 'linux'
                              WHEN regexp_like(log_entries.uri, '.*\-windows\-.*$') THEN 'windows'
                              ELSE NULL END
                 ELSE NULL END                                                                     AS os
           , CASE
                 WHEN regexp_like(log_entries.uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*')
                     THEN split_part(log_entries.uri, '/', 2)
                 WHEN regexp_like(log_entries.uri, '^\/desktop\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*')
                     THEN split_part(log_entries.uri, '/', 3)
                 ELSE NULL END                                                                     AS version
           , nullif(regexp_replace(
                            regexp_replace(regexp_replace(split_part(log_entries.creferrer, '/', 3), '^[w]{3}[.]', '')
                                , '.com[0-9_a-z_A-Z:.]{1,}$', '.com'), ':{1}[0-9]{1,4}$', ''), '') AS server_source
           , log_entries.edge
           , log_entries.bytessent
           , log_entries.method
           , log_entries.host
           , log_entries.status
           , log_entries.creferrer
           , log_entries.useragent
           , log_entries.cs_uri_query
           , log_entries.cookie
           , log_entries.x_edge_result_type
           , log_entries.x_edge_request_id
           , log_entries.x_host_header
           , log_entries.protocol
           , log_entries.cs_bytes
           , log_entries.time_taken
           , log_entries.x_forwarded_for
           , log_entries.ssl_protocol
           , log_entries.ssl_cipher
           , log_entries.x_edge_response_result_type
           , log_entries.cs_protocol_version
           , log_entries.file_status
           , log_entries.file_encrypted_fields
           , o.diff                                                                                AS bytessent_diff_from_uri_mean
           , o.std                                                                                 AS bytessent_std_from_uri_mean
           , o.avg                                                                                 AS bytessent_uri_avg
           , o.first_install_datetime::timestamp                                                   AS first_install_datetime
         FROM {{ source('releases', 'log_entries') }} log_entries
              JOIN download_stats o
                   ON log_entries.logdate::DATE = o.logdate::DATE
                       AND log_entries.cip = o.cip
                       AND log_entries.uri = o.uri
                       AND log_entries.bytessent = o.bytessent
                       AND log_entries.logtime = o.logtime
         WHERE (regexp_like(log_entries.uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') OR
                regexp_like(log_entries.uri, '^\/desktop\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*'))
           AND log_entries.bytessent > 1000000
           AND log_entries.status LIKE '2%'
           AND NOT o.one_std_from_mean
           {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            AND log_entries.logdate::date > (SELECT MAX(logdate) FROM {{ this }})

          {% endif %}
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
                , 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35
     )

SELECT *
FROM downloads
ORDER BY 1 DESC, 2 DESC