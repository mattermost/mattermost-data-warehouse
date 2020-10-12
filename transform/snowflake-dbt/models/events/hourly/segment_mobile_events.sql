{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"preunion"
  })
}}

SELECT
    segment.*
FROM {{ source('mattermost_rn_mobile_release_builds_v2', 'event') }} segment
     LEFT JOIN (
                SELECT user_id, MIN(TIMESTAMP::DATE) AS MIN_DATE
                FROM {{ ref('mobile_events') }}
                GROUP BY 1
              ) rudder
        ON segment.user_id = rudder.user_id AND segment.timestamp::DATE >= rudder.MIN_DATE
WHERE rudder.user_id is NULL
AND segment.timestamp::date <= CURRENT_DATE
AND segment.timestamp::date >= '2019-02-01'
AND COALESCE(type, event) NOT IN ('api_profiles_get_by_ids', 'api_profiles_get_by_usernames','api_profiles_get_in_channel', 'application_backgrounded', 'application_opened')
{% if is_incremental() %}

AND segment.timestamp > (SELECT MAX(timestamp) FROM {{this}})

{% endif %}