{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"preunion"
  })
}}

{% if is_incremental() %}
 WITH max_timestamp AS (
   SELECT MAX(timestamp) - INTERVAL '1 DAY' AS max_time 
   FROM {{ this }} 
   WHERE timestamp <= CURRENT_TIMESTAMP
 ),
  
join_key AS (
  SELECT join_key.ID AS JOIN_KEY
  FROM {{ this }} join_key
  JOIN max_timestamp mt
    ON join_key.timestamp >= mt.max_time
  WHERE TIMESTAMP <= CURRENT_TIMESTAMP
  AND COALESCE(type, event) NOT IN ('api_profiles_get_by_ids', 'api_profiles_get_by_usernames','api_profiles_get_in_channel', 'application_backgrounded', 'application_opened')
)


SELECT e.*
FROM {{ source('mm_telemetry_prod', 'event') }} e
JOIN max_timestamp mt
    ON e.timestamp >= mt.max_time
LEFT JOIN join_key a
  ON e.id = a.JOIN_KEY
WHERE e.timestamp <= CURRENT_TIMESTAMP
AND a.JOIN_KEY IS NULL
AND COALESCE(type, event) NOT IN ('api_profiles_get_by_ids', 'api_profiles_get_by_usernames','api_profiles_get_in_channel', 'application_backgrounded', 'application_opened')

{% else %}
SELECT e.*
FROM {{ source('mm_telemetry_prod', 'event') }} e
WHERE e.timestamp <= CURRENT_TIMESTAMP
AND COALESCE(type, event) NOT IN ('api_profiles_get_by_ids', 'api_profiles_get_by_usernames','api_profiles_get_in_channel', 'application_backgrounded', 'application_opened')
{% endif %}


