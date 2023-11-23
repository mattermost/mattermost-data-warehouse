{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"preunion",
    "snowflake_warehouse": "transform_l"
  })
}}

{% if is_incremental() %}

with rudder as (
 SELECT 
        user_id
      , TO_DATE(MIN(received_at)) AS MIN_DATE
  FROM {{ source('mm_telemetry_prod', 'server') }}
  GROUP BY 1
),

max_time as (
 SELECT 
      MAX(received_at) as max_time 
 FROM {{ this }} 
 WHERE received_at <= CURRENT_TIMESTAMP
),

join_key as (
SELECT 
    join_key.ID AS JOIN_KEY
FROM {{ this }} join_key
JOIN max_time mt
  ON join_key.received_at >= mt.max_time
),

segment as (
SELECT 
    segment.*
FROM {{ source('mattermost2', 'event') }} segment
JOIN max_time
    ON segment.received_at > max_time.max_time
LEFT JOIN rudder
    ON segment.user_id = rudder.user_id 
    AND TO_DATE(segment.received_at) > rudder.MIN_DATE
WHERE rudder.user_id is NULL
AND segment.received_at <= CURRENT_TIMESTAMP
AND segment.received_at >= '2019-02-01'
)

SELECT 
    s.*
FROM segment s
LEFT JOIN JOIN_KEY a
  ON s.id = a.JOIN_KEY
WHERE s.received_at <= CURRENT_TIMESTAMP
AND a.join_key is null

{% else %}

with rudder as (
 SELECT 
      user_id
    , TO_DATE(MIN(received_at)) AS MIN_DATE
  FROM {{ source('mm_telemetry_prod', 'server') }}
  GROUP BY 1
)

SELECT 
    segment.*
FROM {{ source('mattermost2', 'event') }} segment
LEFT JOIN rudder
    ON segment.user_id = rudder.user_id
    AND TO_DATE(segment.received_at) > rudder.MIN_DATE
WHERE rudder.user_id is NULL
AND segment.received_at <= CURRENT_TIMESTAMP
AND segment.received_at >= '2019-02-01'

{% endif %}