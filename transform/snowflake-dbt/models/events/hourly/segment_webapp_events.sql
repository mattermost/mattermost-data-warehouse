{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"hourly"
  })
}}

SELECT
    segment.*
FROM {{ source('mattermost2', 'event') }} segment
     LEFT JOIN (
                SELECT user_id, MIN(TIMESTAMP::DATE) AS MIN_DATE
                FROM {{ source('mm_telemetry_prod', 'event') }}
                GROUP BY 1
              ) rudder
        ON segment.user_id = rudder.user_id AND segment.timestamp::DATE >= rudder.MIN_DATE
WHERE rudder.user_id is NULL
AND segment.timestamp::date <= CURRENT_DATE
AND segment.timestamp::date >= '2019-02-01'
{% if is_incremental() %}

AND segment.timestamp > (SELECT MAX(timestamp) FROM {{this}})

{% endif %}