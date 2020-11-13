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
          WHERE TIMESTAMP::DATE >= (SELECT MAX(timestamp::date) FROM {{ this }} WHERE timestamp <= CURRENT_TIMESTAMP) - INTERVAL '1 DAYS'
          AND timestamp <= CURRENT_TIMESTAMP
          AND COALESCE(type, event) NOT IN ('api_profiles_get_by_ids', 'api_profiles_get_by_usernames','api_profiles_get_in_channel', 'application_backgrounded', 'application_opened')
        ) a
  ON e.id = a.JOIN_KEY
WHERE e.timestamp::date >= (SELECT MAX(timestamp::date) FROM {{ this }}) - INTERVAL '1 DAYS'
AND e.timestamp <= CURRENT_TIMESTAMP
AND a.JOIN_KEY IS NULL
-- Date rudder started sending telemetry
AND timestamp::date >= '2020-05-14'
AND COALESCE(type, event) NOT IN ('api_profiles_get_by_ids', 'api_profiles_get_by_usernames','api_profiles_get_in_channel', 'application_backgrounded', 'application_opened')

{% else %}
WHERE e.timestamp <= CURRENT_TIMESTAMP
{% endif %}


