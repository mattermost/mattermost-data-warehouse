{{config({
    "materialized": "table",
    "schema": "events"
  })
}}

WITH security AS (
    SELECT
        substring(regexp_substr(cs_uri_query, '(^|&)id=([^&]*)'), 5, 100) AS id,
        logdate::date AS date,
        substring(logtime, 1, 2)::integer AS hour,
        CASE WHEN substring(logtime, 1, 2)::integer >= 12 THEN 'B' ELSE 'A' END AS grouping,
        cip AS ip_address,
        substring(edge, 1, 3) AS location, 
        COALESCE( 
            CASE 
                WHEN split_part(regexp_substr(cs_uri_query, 'auc=[0-9]{1,10}'), '=', 2) = '' THEN NULL 
                ELSE split_part(regexp_substr(cs_uri_query, 'auc=[0-9]{1,10}'), '=', 2)::INT
            END, 0) AS active_user_count, 
        COALESCE(
            CASE
                WHEN split_part(regexp_substr(cs_uri_query, '[^a]uc=[0-9]{1,10}'),'=',2) = '' THEN NULL
                ELSE split_part(regexp_substr(cs_uri_query, '[^a]uc=[0-9]{1,10}'),'=',2)::int
            END, 0) AS user_count,
        COALESCE(
            CASE
                WHEN split_part(regexp_substr(cs_uri_query, 'tc=[0-9]{1,10}'),'=',2) = '' THEN NULL
                ELSE split_part(regexp_substr(cs_uri_query, 'tc=[0-9]{1,10}'),'=',2)::int
            END, 0) AS team_count,
        substring(regexp_substr(cs_uri_query, '(^|&)b=([^&]*)'), 4, 100) AS version,
                CASE
                    WHEN position(regexp_substr(cs_uri_query, '(^|&)b=([^&]*)'), '_BUILD_NUMBER_') > 1 THEN true
                    ELSE false
                END AS dev_build, 
        substring(regexp_substr(cs_uri_query, '(^|&)db=([^&]*)'), 5, 100) AS db_type, 
        substring(regexp_substr(cs_uri_query, '(^|&)os=([^&]*)'), 5, 100) AS os_type,
        CASE
            WHEN substring(regexp_substr(cs_uri_query, '(^|&)ut=([^&]*)'), 5, 100) = '1' THEN true
            ELSE false
        END AS ran_tests,
        (logdate || ' ' || logtime)::TIMESTAMP                            AS timestamp 
    FROM {{ source('diagnostics', 'log_entries') }}
    WHERE uri = '/security'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
)

SELECT * FROM security