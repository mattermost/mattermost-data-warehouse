{{config({
    "materialized": 'table',
    "schema": "mattermost"
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
                            'Mattermost'
                            )
        AND l.license_activation_date is not null
    LEFT JOIN seed_file sf
        ON s.server_id = sf.server_id
    WHERE sf.server_id is null
    GROUP BY 1, 2
),

version_exclusions AS (
        SELECT
          CASE WHEN sec.dev_build = 1 or sec.ran_tests = 1 THEN 'Dev Build/Ran Tests'
               WHEN sec.ip_address = '194.30.0.184' THEN 'Restricted IP'
               WHEN (
                    regexp_substr(COALESCE(sec.version, '1.2.3'), '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}$') is null
                    AND regexp_substr(COALESCE(sec.version, '1.2.3'), '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}$') is null
                    AND regexp_substr(COALESCE(sec.version, '1.2.3'), '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{10,}$') is null
                ) THEN 'Custom Build Version Format'
               WHEN sec.user_count < sec.active_user_count THEN 'Active Users > Registered Users'
               ELSE NULL END    AS REASON
        , trim(sec.server_id)                AS server_id
    FROM {{ ref('security') }} sec
    LEFT JOIN seed_file sf
        ON trim(sec.server_id) = sf.server_id
    LEFT JOIN license_exclusions le
        ON trim(sec.server_id) = le.server_id
    LEFT JOIN {{ ref('server_fact') }} s
        ON trim(sec.server_id) = s.server_id
    WHERE sf.server_id is NULL
    AND le.server_id is null
    AND s.server_id is null
    AND (sec.dev_build = 1
      OR sec.ran_tests = 1
      OR (regexp_substr(COALESCE(sec.version, '1.2.3'), '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}$') is null
          AND regexp_substr(COALESCE(sec.version, '1.2.3'), '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}$') is null
          AND regexp_substr(COALESCE(sec.version, '1.2.3'), '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}(cloud(-|\.){1}|ee_live{1})') is null
          AND regexp_substr(COALESCE(sec.version, '1.2.3'), '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{10,}$') is null)
      OR sec.ip_address = '194.30.0.184'
      OR sec.user_count < (sec.active_user_count + (sec.user_count * .2)))
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
    LEFT JOIN {{ ref('server_fact') }} fact
        ON TRIM(s.server_id) = TRIM(fact.server_id)
    WHERE sf.server_id is NULL
    AND ve.server_id is NULL
    AND le.server_id is NULL
    AND fact.installation_id IS NULL
    GROUP BY 1, 2
),

cloud_servers AS (
    SELECT 
        CASE WHEN regexp_substr(s.first_server_version, '[0-9]{1,2}.{1}[0-9]{1,2}.{1}[0-9]{1,2}$') IS NULL THEN 'Version Format' 
            WHEN (lower(SPLIT_PART(coalesce(c.email, 'test@test.com'), '@', 2)) IN ('mattermost.com', 'adamcgross.com', 'hulen.com')
            AND coalesce(c.email, 'test@test.com') != 'jason@mattermost.com')
            OR lower(coalesce(c.email, 'test@test.com')) IN ('ericsteven1992@gmail.com', 'eric.nelson720@gmail.com') THEN 'Internal Email'
            WHEN sb.cws_installation IS NULL THEN 'No Blapi/Stripe Installation Found'
            ELSE NULL END as reason
      , s.server_id
     FROM {{ ref('server_fact') }} s
    LEFT JOIN {{ ref('subscriptions') }} sb
        ON s.installation_id = sb.cws_installation
    LEFT JOIN {{ ref('customers') }} c
        ON sb.customer = c.id
    LEFT JOIN seed_file sf
        ON trim(s.server_id) = trim(sf.server_id)
    LEFT JOIN version_exclusions ve
        ON trim(s.server_id) = trim(ve.server_id)
    LEFT JOIN license_exclusions le
        ON trim(s.server_id) = trim(le.server_id)
    LEFT JOIN ip_exclusions ip
        ON trim(s.server_id) = trim(ip.server_id)
     WHERE ((regexp_substr(s.first_server_version, '[0-9]{1,2}.{1}[0-9]{1,2}.{1}[0-9]{1,2}$') IS NULL
            OR 
            regexp_substr(s.first_server_version, '[0-9]{1,2}.{1}[0-9]{1,2}.{1}[0-9]{1,2}.{1}.cloud[/-]ga') IS NULL)
     OR (lower(SPLIT_PART(coalesce(c.email, 'test@test.com'), '@', 2)) IN ('mattermost.com', 'adamcgross.com', 'hulen.com')
        AND coalesce(c.email, 'test@test.com') != 'jason@mattermost.com')
     OR lower(coalesce(c.email, 'test@test.com')) IN ('ericsteven1992@gmail.com', 'eric.nelson720@gmail.com')
     OR sb.cws_installation is null)
     AND s.installation_id IS NOT NULL
     AND sf.server_id is NULL
    AND ve.server_id is NULL
    AND le.server_id is NULL
    AND ip.server_id is NULL
    AND CASE WHEN regexp_substr(s.first_server_version, '[0-9]{1,2}.{1}[0-9]{1,2}.{1}[0-9]{1,2}$') IS NULL THEN 'Version Format' 
            WHEN (lower(SPLIT_PART(coalesce(c.email, 'test@test.com'), '@', 2)) IN ('mattermost.com', 'adamcgross.com', 'hulen.com')
            AND coalesce(c.email, 'test@test.com') != 'jason@mattermost.com')
            OR lower(coalesce(c.email, 'test@test.com')) IN ('ericsteven1992@gmail.com', 'eric.nelson720@gmail.com') THEN 'Internal Email'
            WHEN sb.cws_installation IS NULL THEN 'No Blapi/Stripe Installation Found'
            ELSE NULL END IS NOT NULL
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
    UNION ALL
    SELECT *
    FROM cloud_servers
)
SELECT *
FROM excludable_servers