WITH first_server_version AS (
    SELECT 
        server_id,
        MIN(event_date) AS min_event_date
    FROM {{ ref('int_nps_score') }}
    GROUP BY all
),
spined AS (
    SELECT 
        date_day::date AS activity_date,
        server_id AS server_id
    FROM first_server_version fsd
    LEFT JOIN {{ ref('telemetry_days') }} td 
        ON date_day::date >= min_event_date
),
server_version_cte AS (
    SELECT 
        sp.activity_date,
        sp.server_id,
        MAX(nps_score.server_version) AS server_version
    FROM spined sp 
    LEFT JOIN {{ ref('int_nps_score') }} nps_score 
        ON sp.server_id = nps_score.server_id AND sp.activity_date = nps_score.event_date
    GROUP BY all
)
SELECT 
    activity_date,
    server_id,
    FIRST_VALUE(nps_score.server_version IGNORE NULLS) OVER (
        PARTITION BY sp.server_id 
        ORDER BY activity_date DESC
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS server_version
FROM server_version_cte
