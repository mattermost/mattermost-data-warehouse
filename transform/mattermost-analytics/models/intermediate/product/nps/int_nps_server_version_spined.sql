WITH first_server_version AS (
    SELECT 
        server_id,
        server_version,
        MIN(event_date) AS min_event_date
    FROM {{ ref('int_nps_score') }}
    WHERE event_date >= '{{ var('nps_start_date')}}'
    GROUP BY all
),
spined AS (
    SELECT 
        date_day::date AS activity_date,
        server_id AS server_id,
        case when date_day::date = min_event_date then server_version end as server_version
    FROM first_server_version fsd
    LEFT JOIN {{ ref('nps_days') }} td 
        ON date_day::date >= min_event_date
) SELECT 
    activity_date,
    server_id,
    FIRST_VALUE(server_version IGNORE NULLS) OVER (
        PARTITION BY server_id 
        ORDER BY activity_date DESC
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS server_version
FROM spined
QUALIFY ROW_NUMBER() OVER (PARTITION BY activity_date, server_id ORDER BY activity_date DESC) = 1
