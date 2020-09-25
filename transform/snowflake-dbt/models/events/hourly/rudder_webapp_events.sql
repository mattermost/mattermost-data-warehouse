{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"hourly"
  })
}}

SELECT e.*
FROM {{ source('mm_telemetry_prod', 'event') }} e
{% if is_incremental() %}

LEFT JOIN 
        (
          SELECT ID AS JOIN_KEY
          FROM {{ this }}
          WHERE TIMESTAMP::DATE >= CURRENT_DATE - INTERVAL '2 DAYS'
          GROUP BY 1
        ) a
  ON e.id = a.JOIN_KEY
WHERE e.timestamp::date >= CURRENT_DATE - INTERVAL '2 DAYS'
AND e.timestamp <= CURRENT_TIMESTAMP
AND a.JOIN_KEY IS NULL
-- Date rudder started sending telemetry
AND timestamp::date >= '2020-05-14'

{% else %}
WHERE e.timestamp <= CURRENT_TIMESTAMP
{% endif %}


