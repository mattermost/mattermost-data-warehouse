{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"preunion",
    "unique_key":"id"
  })
}}

with cloud_portal_pageview_events AS (
SELECT 
    p.COLUMN_NAME,
    p.ANONYMOUS_ID,
    p.CHANNEL,
    p.CONTEXT_APP_BUILD,
    p.CONTEXT_APP_NAME,
    p.CONTEXT_APP_NAMESPACE,
    p.CONTEXT_APP_VERSION,
    p.CONTEXT_IP,
    p.CONTEXT_LIBRARY_NAME,
    p.CONTEXT_LIBRARY_VERSION,
    p.CONTEXT_LOCALE,
    p.CONTEXT_OS_NAME,
    p.CONTEXT_OS_VERSION,
    p.CONTEXT_PAGE_PATH,
    p.CONTEXT_PAGE_REFERRER,
    p.CONTEXT_PAGE_SEARCH,
    p.CONTEXT_PAGE_TITLE,
    p.CONTEXT_PAGE_URL,
    p.CONTEXT_SCREEN_DENSITY,
    p.CONTEXT_USERAGENT,
    p.ID,
    p.NAME,
    p.ORIGINAL_TIMESTAMP,
    p.PATH,
    p.RECEIVED_AT,
    p.REFERRER,
    p.SEARCH,
    p.SENT_AT,
    p.TIMESTAMP,
    p.TITLE,
    p.URL,
    p.USER_ID,
    p.UUID_TS,
    , CASE WHEN split_part(path, '/', 3) = 'signup' THEN 'pageview_getting_started'
        WHEN split_part(path, '/', 3) = 'verify-email' THEN 'pageview_verify_email' 
        WHEN split_part(path, '/', 3) = 'company-name' THEN 'pageview_company_name'
        WHEN split_part(path, '/', 3) = 'create-workspace' THEN 'pageview_create_workspace'
        ELSE NULL END as type
    , CASE WHEN split_part(path, '/', 3) = 'signup' THEN 'cloud_account_creation_start'
        WHEN split_part(path, '/', 3) = 'verify-email' THEN 'cloud_account_creation_verify' 
        WHEN split_part(path, '/', 3) = 'company-name' THEN 'cloud_account_creation_company'
        WHEN split_part(path, '/', 3) = 'create-workspace' THEN 'cloud_account_creation_workspace'
        ELSE NULL END as category
FROM {{ source('portal_prod', 'pages') }} p
{% if is_incremental() %}
LEFT JOIN 
        (
          SELECT DISTINCT ID AS JOIN_KEY
          FROM {{ this }}
          WHERE TIMESTAMP::DATE >= (SELECT MAX(timestamp::date) FROM {{ this }}) - INTERVAL '1 DAYS'
          AND path like '%cloud%'
        ) a
  ON p.id = a.JOIN_KEY
{% endif %}
WHERE path like '%cloud%'
AND CASE WHEN split_part(path, '/', 3) = 'signup' THEN 'pageview_getting_started'
        WHEN split_part(path, '/', 3) = 'verify-email' THEN 'pageview_verify_email' 
        WHEN split_part(path, '/', 3) = 'company-name' THEN 'pageview_company_name'
        WHEN split_part(path, '/', 3) = 'create-workspace' THEN 'pageview_create_workspace'
        ELSE NULL END IS NOT NULL
{% if is_incremental() %}

AND p.timestamp::date >= (SELECT MAX(timestamp::date) FROM {{ this }}) - INTERVAL '1 DAYS'
AND a.join_key is NULL

{% endif %}
)

SELECT *
FROM cloud_portal_pageview_events