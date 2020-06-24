{{config({
    "materialized": 'table',
    "schema": "mattermost",
    "unique_key":'server_id',
  })
}}

WITH seed_file AS (
    SELECT
        reason
      , trim(server_id) AS server_id
    FROM {{ ref('excludable_servers_seed') }}
    GROUP BY 1, 2
),

license_exclusions AS (
    SELECT
        'internal_license' AS reason
      , trim(s.server_id) AS server_id
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
        , trim(sec.id)                AS server_id
    FROM {{ ref('security') }} sec
    LEFT JOIN seed_file sf
        ON trim(sec.id) = sf.server_id
    LEFT JOIN license_exclusions le
        ON trim(sec.id) = le.server_id
    LEFT JOIN {{ ref('server_fact') }} s
        ON trim(sec.id) = s.server_id
    WHERE sf.server_id is NULL
    AND le.server_id is null
    AND s.server_id is null
    AND (sec.dev_build = 1
      OR sec.ran_tests = 1
      OR sec.version NOT LIKE '_.%._._.%._'
      OR sec.ip_address = '194.30.0.184'
      OR sec.user_count < sec.active_user_count)
      AND sec.date <= CURRENT_DATE - INTERVAL '1 DAY'
    GROUP BY 1, 2
),

ip_exclusions AS (
    SELECT 
        'restricted_ip_range'
       , s.server_id
    FROM {{ ref('server_daily_details') }} s
    JOIN (
        SELECT
          ip_address
        , e.reason
        , count(DISTINCT s.server_id) AS COUNT
        FROM {{ ref('server_daily_details')}}    s
            JOIN license_exclusions e
                ON trim(s.server_id) = e.server_id
        WHERE nullif(s.ip_address, '') IS NOT NULL
        GROUP BY 1, 2) e
        ON s.ip_address = e.ip_address
    LEFT JOIN seed_file sf
        ON trim(s.server_id) = trim(sf.server_id)
    LEFT JOIN version_exclusions ve
        ON trim(s.server_id) = trim(ve.server_id)
    LEFT JOIN license_exclusions le
        ON trim(s.server_id) = trim(le.server_id)
    WHERE sf.server_id is null
    AND ve.server_id is NULL
    AND le.server_id is NULL
    GROUP BY 1, 2
),

excludable_servers AS (
    SELECT *
    FROM seed_file
    UNION ALL
    SELECT *
    FROM license_exclusions
    UNION ALL
    SELECT *
    FROM version_exclusions
    UNION ALL
    SELECT *
    FROM ip_exclusions
)
SELECT *
FROM excludable_servers