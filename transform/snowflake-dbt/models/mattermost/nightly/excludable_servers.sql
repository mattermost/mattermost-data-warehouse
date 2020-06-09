{{config({
    "materialized": 'incremental',
    "schema": "mattermost",
    "unique_key":'server_id'
  })
}}

WITH seed_file AS (
    SELECT *
    FROM {{ ref('excludable_servers_seed') }}
),

license_exclusions AS (
    SELECT
        'internal_license' AS reason
      , s.server_id
    FROM {{ ref('server_fact') }} s
    JOIN {{ ref('licenses') }} l
        ON s.server_id = l.server_id
        AND trim(l.company) in (
                            'Mattermost Developers',
                            'Mattermost SA - INTERNAL',
                            'Mattermost  Inc.',
                            'Mattermost Cloud',
                            'Mattermost'
                            )
    LEFT JOIN seed_file sf
        ON s.server_id = sf.server_id
    WHERE sf.server_id is null
    GROUP BY 1, 2
),

version_exclusions AS (
        SELECT
          CASE WHEN sec.dev_build = 1 or sec.ran_tests = 1 THEN 'Dev Build/Ran Tests'
               WHEN sec.ip_address = '194.30.0.184' THEN 'Restricted IP'
               WHEN sec.version NOT LIKE '_.%._._.%._' THEN 'Custom Build Version Format'
               WHEN sec.user_count < sec.active_user_count THEN 'Active Users > Registered Users'
               ELSE NULL END    AS REASON
        , sec.id                AS server_id
    FROM {{ ref('security') }} sec
    WHERE (sec.dev_build = 1
      OR sec.ran_tests = 1
      OR sec.version NOT LIKE '_.%._._.%._'
      OR sec.ip_address = '194.30.0.184'
      OR sec.user_count < sec.active_user_count)
      AND sec.date <= CURRENT_DATE - INTERVAL '1 DAY'
)

excludable_servers AS (
    SELECT *
    FROM seed_file
    UNION ALL
    SELECT *
    FROM license_exclusions
)
SELECT *
FROM excludable_servers
{% if is_incremental() %}

WHERE server_id NOT IN (SELECT server_id from {{ this }})

{% endif %}