{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":["nightly"]
  })
}}

with cloud_portal_pageview_events AS (
SELECT 
    {{ dbt_utils.star(from=source('portal_prod', 'pages'), except=["CATEGORY"]) }}
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
          WHERE received_at > (SELECT MAX(received_at) FROM {{ this }})
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

AND p.received_at > (SELECT MAX(received_at) FROM {{ this }})
AND a.join_key is NULL

{% endif %}
)

SELECT *
FROM cloud_portal_pageview_events