WITH first_server_version AS 
(
    SELECT 
        server_id
        , min(event_date) min_event_date
    FROM {{ ref('int_nps_score') }}
    WHERE event_date >= '{{ var('telemetry_start_date')}}'
        GROUP BY all
)
, spined AS (
    SELECT 
        date_day::DATE AS activity_date
        , server_id AS server_id
        , FIRST_VALUE(server_version IGNORE NULLS) OVER (
            PARTITION BY server_id 
            ORDER BY activity_date DESC
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
        AS server_version
    FROM first_server_version fsd
    LEFT JOIN {{ ref('telemetry_days') }} td 
        ON date_day::DATE >= min_event_date
) SELECT * FROM spined 
    ORDER BY activity_date DESC


