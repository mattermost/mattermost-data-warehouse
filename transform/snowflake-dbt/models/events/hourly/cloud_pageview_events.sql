{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"preunion",
    "unique_key":"id"
  })
}}

with cloud_pageview_events AS (
SELECT 
    p.*, 
    p.name as type
FROM {{ source('mm_telemetry_prod', 'pages') }} p
{% if is_incremental() %}
LEFT JOIN 
        (
          SELECT DISTINCT ID AS JOIN_KEY
          FROM {{ this }}
          WHERE TIMESTAMP::DATE >= (SELECT MAX(timestamp::date) FROM {{ this }}) - INTERVAL '1 DAYS'
          AND name != 'ApplicationLoaded'
        ) a
  ON p.id = a.JOIN_KEY
{% endif %}
WHERE name != 'ApplicationLoaded'
{% if is_incremental() %}

AND p.timestamp::date >= (SELECT MAX(timestamp::date) FROM {{ this }}) - INTERVAL '1 DAYS'
AND a.join_key is NULL

{% endif %}
)

SELECT *
FROM cloud_pageview_events