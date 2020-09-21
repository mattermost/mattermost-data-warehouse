{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"hourly"
  })
}}

SELECT *
FROM {{ source('mm_telemetry_prod', 'event') }}
WHERE timestamp <= CURRENT_TIMESTAMP
-- Date rudder started sending telemetry
AND timestamp::date >= '2020-05-14'
{% if is_incremental() %}

AND timestamp::date > (SELECT MAX(timestamp) FROM {{this}})

{% endif %}