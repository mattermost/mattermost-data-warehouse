{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"hourly",
    "unique_key":"id"
  })
}}

SELECT
    segment.*
FROM {{ source('mattermost_rn_mobile_release_builds_v2', 'event') }} segment
     LEFT JOIN {{ ref('mobile_events') }} rudder
        ON segment.user_actual_id = rudder.user_actual_id AND rudder.timestamp::DATE = segment.timestamp::DATE AND
             rudder.type = segment.type
WHERE rudder.user_actual_id is NULL
AND segment.timestamp::date <= CURRENT_DATE
AND segment.timestamp::date >= '2019-02-01'
{% if is_incremental() %}

AND segment.timestamp > (SELECT MAX(timestamp) FROM {{this}})

{% endif %}