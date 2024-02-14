WITH base_cte AS (
    SELECT DISTINCT
        td.date_day::date AS activity_date,
        b.server_id AS server_id,
        b.user_id AS user_id
    FROM
         {{ ref('telemetry_days') }} td
    LEFT JOIN
        {{ ref('int_nps_score') }} b ON td.date_day::date >= event_date
), spined AS (
    SELECT 
        activity_date,
        nps.server_id,
        nps.user_id,
        MAX(nps.user_role) AS user_role,
        MAX(nps.server_version) AS server_version,
        SUM(CASE WHEN activity_date = nps.event_date THEN nps.score END) AS score
    FROM 
        base_cte a 
    JOIN 
        {{ ref('int_nps_score') }} nps
    ON 
        a.server_id = nps.server_id 
        AND a.user_id = nps.user_id 
        AND activity_date >= event_date
    GROUP BY 
        activity_date,
        nps.server_id,
        nps.user_id
) 
SELECT 
    activity_date,
    server_id,
    user_id,
    user_role,
    server_version,
    FIRST_VALUE(score IGNORE NULLS) OVER (PARTITION BY server_id, user_id, user_role ORDER BY activity_date DESC
            ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING) AS score
FROM 
    spined 
ORDER BY 
    activity_date DESC
