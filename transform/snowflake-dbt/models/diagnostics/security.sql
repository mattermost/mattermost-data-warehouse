{{config({
    "materialized": "table",
    "schema": "finance"
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
                WHEN substring(regexp_substr(cs_uri_query, '(^|&)auc=([^&]*)'), 5, 100) = '' THEN NULL 
                ELSE substring(regexp_substr(cs_uri_query, '(^|&)auc=([^&]*)'), 5, 100)::INT
            END, 0) AS active_user_count, 
        COALESCE(
            CASE
                WHEN "substring"(regexp_substr(cs_uri_query, '(^|&)uc=([^&]*)'), 5, 100) = '' THEN NULL
                ELSE "substring"(regexp_substr(cs_uri_query, '(^|&)uc=([^&]*)'), 5, 100)::int
            END, 0) AS user_count,
        substring(regexp_substr(cs_uri_query, '(^|&)b=([^&]*)'), 4, 100) AS version,
                CASE
                    WHEN "position"(regexp_substr(cs_uri_query, '(^|&)b=([^&]*)'), '_BUILD_NUMBER_') > 1 THEN true
                    ELSE false
                END AS dev_build, 
        substring(regexp_substr(cs_uri_query, '(^|&)db=([^&]*)'), 5, 100) AS db_type, 
        substring(regexp_substr(cs_uri_query, '(^|&)os=([^&]*)'), 5, 100) AS os_type,
        CASE
            WHEN "substring"(regexp_substr(cs_uri_query, '(^|&)ut=([^&]*)'), 5, 100) = '1' THEN true
            ELSE false
        END AS ran_tests
    FROM {{ source('diagnostics', 'log_entries') }}
    WHERE uri = '/security'
)

SELECT * FROM security