{{config({
    "materialized": "incremental",
    "schema": "mattermost",
    "tags": ["union", "nightly", "deprecated"],
    "snowflake_warehouse": "transform_l",
    "unique_key":"id"
  })
}}

WITH first_active AS (
    SELECT
        uet.user_actual_id
      , sf.server_id
      , {{ dbt_utils.surrogate_key(['uet.user_actual_id', 'sf.server_id'])}} AS id
      , MIN(uet.timestamp) AS first_active_timestamp
    FROM {{ ref('user_events_telemetry') }} uet
         JOIN {{ ref('server_fact') }}  sf
              ON COALESCE(uet.user_id, IFF(LENGTH(uet.context_server) < 26, NULL, uet.context_server),
                          IFF(LENGTH(uet.context_traits_userid) < 26, NULL, uet.context_traits_userid),
                          IFF(LENGTH(uet.context_server) < 26, NULL, uet.context_server)) = sf.server_id
            AND uet.timestamp::date >= sf.first_active_date::date
            AND sf.first_active_date::date >= '2020-02-01'
    {% if is_incremental() %}
    WHERE uet.timestamp < CURRENT_TIMESTAMP
      AND {{ dbt_utils.surrogate_key(['uet.user_actual_id', 'sf.server_id'])}} NOT IN (SELECT id FROM {{this}} group by 1)
      AND uet.user_actual_id IS NOT NULL
      AND sf.installation_id is null
    {% else %}
    WHERE uet.timestamp < CURRENT_TIMESTAMP
      AND uet.user_actual_id IS NOT NULL
      AND sf.installation_id is null
    {% endif %}
    GROUP BY 1, 2, 3
                     )

SELECT
      first_active.first_active_timestamp
  ,   first_active.server_id
  ,   first_active.user_actual_id AS user_id
  ,   CASE WHEN nullif(COUNT(DISTINCT uet2.user_actual_id),0) IS NOT NULL THEN TRUE ELSE FALSE END AS retained_28day_users
  ,   MAX(uet2.timestamp) AS retained_28day_timestamp
  ,   first_active.id
FROM first_active
     LEFT JOIN {{ ref('user_events_telemetry') }} uet2
          ON uet2.user_actual_id = first_active.user_actual_id
              AND uet2.timestamp between first_active.first_active_timestamp + interval '672 hours' and first_active.first_active_timestamp + interval '696 hours'
WHERE first_active.first_active_timestamp <= CURRENT_TIMESTAMP - INTERVAL '696 HOURS'
group by 1, 2, 3, 6
