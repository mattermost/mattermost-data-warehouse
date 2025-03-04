WITH first_server_version AS (
    SELECT 
        server_id,
        MIN(event_date) AS min_event_date
    FROM {{ ref('int_nps_score') }}
    WHERE event_date >= '{{ var('nps_start_date')}}'
    group by all
), spined AS (
    SELECT 
        date_day::date AS activity_date,
        server_id AS server_id
    FROM first_server_version fsd
    LEFT JOIN {{ ref('nps_days') }} td 
        ON date_day::date >= min_event_date
),
server_version_cte AS (
    SELECT 
        sp.activity_date,
        sp.server_id,
        nps_score.server_version_full as server_version_full
    FROM spined sp 
    LEFT JOIN {{ ref('int_nps_score') }} nps_score 
        ON sp.server_id = nps_score.server_id AND sp.activity_date = nps_score.event_date
    -- fetch the last server_version of the given day if there are multiple server_versions reported on the same day
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sp.activity_date, sp.server_id ORDER BY timestamp DESC) = 1
)
SELECT 
    activity_date,
    server_id,
    FIRST_VALUE(server_version_full IGNORE NULLS) OVER (
        PARTITION BY server_id 
        ORDER BY activity_date DESC
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS server_version_full
FROM server_version_cte
ORDER BY ACTIVITY_DATE DESC