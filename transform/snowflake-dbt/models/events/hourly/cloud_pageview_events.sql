{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":["nightly"]
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
          WHERE received_at > (SELECT MAX(received_at) FROM {{ this }})
          AND name != 'ApplicationLoaded'
        ) a
  ON p.id = a.JOIN_KEY
{% endif %}
WHERE name != 'ApplicationLoaded'
{% if is_incremental() %}

AND p.received_at > (SELECT MAX(received_at) FROM {{ this }})
AND a.join_key is NULL

{% endif %}
)

SELECT *
FROM cloud_pageview_events