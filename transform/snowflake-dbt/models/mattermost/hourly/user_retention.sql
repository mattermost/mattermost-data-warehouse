{{config({
    "materialized": "table",
    "schema": "mattermost",
    "tags":["nightly"],
    "unique_key":"id"
  })
}}

WITH first_active AS (
    SELECT
        uet.user_actual_id
      , sf.server_id
      , {{ dbt_utils.surrogate_key(['uet.user_actual_id', 'sf.server_id'])}} AS id
      , sf.installation_id
      , sf.first_server_edition
      , MIN(uet.timestamp) AS first_active_timestamp
    FROM {{ ref('user_events_telemetry') }} uet
         JOIN {{ ref('server_fact') }}  sf
              ON COALESCE(uet.user_id, IFF(LENGTH(uet.context_server) < 26, NULL, uet.context_server),
                          IFF(LENGTH(uet.context_traits_userid) < 26, NULL, uet.context_traits_userid),
                          IFF(LENGTH(uet.context_server) < 26, NULL, uet.context_server)) = sf.server_id
            AND uet.timestamp::date >= sf.first_active_date::date
            AND sf.first_active_date::date >= '2020-02-01'
    WHERE uet.timestamp < CURRENT_TIMESTAMP
      AND uet.user_actual_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
)
SELECT DISTINCT 
      first_active.first_active_timestamp
  ,   first_active.server_id
  ,   first_active.user_actual_id AS user_id
  ,   first_active.installation_id AS installation_id
  ,   first_active.first_server_edition AS first_server_edition
  ,   first_active.id
    ,   CASE WHEN uet2.timestamp between first_active.first_active_timestamp AND first_active.first_active_timestamp + INTERVAL '1 DAY'
                    THEN TRUE ELSE FALSE END AS retention_0day_flag
  ,   CASE WHEN uet2.timestamp between first_active.first_active_timestamp + INTERVAL '1 DAY' 
  AND first_active.first_active_timestamp + INTERVAL '2 DAYS'
                    THEN TRUE ELSE FALSE END AS retention_1day_flag
  ,   CASE WHEN uet2.timestamp between first_active.first_active_timestamp + INTERVAL '2 DAYS' 
  AND first_active.first_active_timestamp + INTERVAL '7 DAYS'
                    THEN TRUE ELSE FALSE END AS retention_7day_flag
  ,   CASE WHEN uet2.timestamp between first_active.first_active_timestamp + INTERVAL '7 DAYS' 
  AND first_active.first_active_timestamp + INTERVAL '14 DAYS'
                    THEN TRUE ELSE FALSE END AS retention_14day_flag
  ,   CASE WHEN uet2.timestamp between first_active.first_active_timestamp + INTERVAL '14 DAYS' 
  AND first_active.first_active_timestamp + INTERVAL '28 DAYS'
                    THEN TRUE ELSE FALSE END AS retention_28day_flag
FROM first_active
     LEFT JOIN {{ ref('user_events_telemetry') }} uet2
          ON uet2.user_actual_id = first_active.user_actual_id
