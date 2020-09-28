{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"preunion"
  })
}}

SELECT e.*
FROM {{ source('mm_telemetry_prod', 'event') }} e
{% if is_incremental() %}

LEFT JOIN 
        (
          SELECT DISTINCT ID AS JOIN_KEY
          FROM {{ this }}
          WHERE TIMESTAMP::DATE >= (SELECT MAX(timestamp::date) FROM {{ this }}) - INTERVAL '1 DAYS'
        ) a
  ON e.id = a.JOIN_KEY
WHERE e.timestamp::date >= (SELECT MAX(timestamp::date) FROM {{ this }}) - INTERVAL '1 DAYS'
AND e.timestamp <= CURRENT_TIMESTAMP
AND a.JOIN_KEY IS NULL
-- Date rudder started sending telemetry
AND timestamp::date >= '2020-05-14'

{% else %}
WHERE e.timestamp <= CURRENT_TIMESTAMP
{% endif %}


