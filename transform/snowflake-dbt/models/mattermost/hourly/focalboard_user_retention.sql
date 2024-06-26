{{config({
    "materialized": "table",
    "schema": "mattermost",
    "tags":["nightly"],
    "unique_key":"id"
  })
}}

WITH first_active AS (
    SELECT
        fet.user_actual_id
      , sf.server_id
      , {{ dbt_utils.surrogate_key(['fet.user_actual_id', 'sf.server_id'])}} AS id
      , sf.installation_id
      , sf.first_server_edition
      , MIN(fet.original_timestamp) AS first_active_timestamp
    FROM {{ ref('focalboard_event_telemetry') }} fet
         JOIN {{ ref('server_fact') }}  sf
              ON fet.user_id = sf.server_id
            AND fet.original_timestamp::date >= sf.first_active_date::date
            AND fet.original_timestamp::date >= '2021-10-11'
    WHERE fet.timestamp < CURRENT_TIMESTAMP
      AND fet.user_actual_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
), q2 as (
SELECT DISTINCT
      first_active.first_active_timestamp
  ,   first_active.server_id
  ,   first_active.user_actual_id AS user_id
  ,   first_active.installation_id AS installation_id
  ,   first_active.first_server_edition AS first_server_edition
  ,   first_active.id
  ,   CASE WHEN (MAX(fet2.ORIGINAL_TIMESTAMP::DATE) - first_active.first_active_timestamp::DATE) = 0 THEN 'AGE 0'
  WHEN (MAX(fet2.ORIGINAL_TIMESTAMP::DATE) - first_active.first_active_timestamp::DATE) > 0 
  AND (MAX(fet2.ORIGINAL_TIMESTAMP::DATE) - first_active.first_active_timestamp::DATE) < 7 THEN 'AGE 1 - 6'
  WHEN (MAX(fet2.ORIGINAL_TIMESTAMP::DATE) - first_active.first_active_timestamp::DATE) >=7 
  AND (MAX(fet2.ORIGINAL_TIMESTAMP::DATE) - first_active.first_active_timestamp::DATE) < 28 THEN 'AGE 7 - 27'
  WHEN (MAX(fet2.ORIGINAL_TIMESTAMP::DATE) - first_active.first_active_timestamp::DATE) >= 28 THEN 'AGE 28+' END as AGE
   , MAX(fet2.ORIGINAL_TIMESTAMP::DATE) as MAX_ORIGINAL_TIMESTAMP
FROM first_active
     LEFT JOIN {{ ref('focalboard_event_telemetry') }} fet2
          ON fet2.user_actual_id = first_active.user_actual_id
GROUP BY first_active.first_active_timestamp
  ,   first_active.server_id
  ,   first_active.user_actual_id 
  ,   first_active.installation_id 
  ,   first_active.first_server_edition 
  ,   first_active.id
) SELECT DISTINCT
      q2.first_active_timestamp
  ,   q2.server_id
  ,   q2.user_id
  ,   q2.installation_id
  ,   q2.first_server_edition
  ,   q2.id
  ,   q2.AGE
  ,   q2.MAX_ORIGINAL_TIMESTAMP
  ,   fet3.type as type
FROM q2 q2
     LEFT JOIN {{ ref('focalboard_event_telemetry') }} fet3
          ON fet3.user_actual_id = q2.user_id
