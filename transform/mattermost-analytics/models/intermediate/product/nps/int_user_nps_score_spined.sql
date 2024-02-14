WITH first_score_day AS (
    SELECT 
        server_id,
        user_id,
        MIN(event_date) AS min_event_date
    FROM 
        {{ ref('int_nps_score') }}
    GROUP BY 
        ALL
), spined AS (
    SELECT 
        date_day::date AS activity_date,
        server_id AS server_id,
        user_id AS user_id
    FROM 
        first_score_day fsd
    LEFT JOIN 
        {{ ref('telemetry_days') }} td 
    ON 
        date_day::date >= min_event_date
), score_cte AS (
    SELECT 
        sp.activity_date,
        sp.server_id,
        sp.user_id,
        nps_score.score,
        COALESCE(nps_score.server_version, FIRST_VALUE(nps_score.server_version IGNORE NULLS) OVER (
            PARTITION BY sp.server_id, sp.user_id 
            ORDER BY activity_date DESC
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            ) AS server_version,
        COALESCE(nps_score.user_role, FIRST_VALUE(nps_score.user_role IGNORE NULLS) OVER (
            PARTITION BY sp.server_id, sp.user_id 
            ORDER BY activity_date DESC
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            ) AS user_role,
        CASE WHEN nps_score.score > 8 THEN 1 ELSE 0 END AS promoters,
        CASE WHEN nps_score.score < 7 THEN 1 ELSE 0 END AS detractors,
        CASE WHEN nps_score.score > 6 AND nps_score.score < 9 THEN 1 ELSE 0 END AS passives,
        CASE WHEN nps_score.score IS NOT NULL THEN 1 ELSE 0 END AS nps_users,
        FIRST_VALUE(score IGNORE NULLS) OVER (
            PARTITION BY sp.server_id, sp.user_id 
            ORDER BY activity_date DESC
            ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING
        ) AS score_last_90_days,
        CASE WHEN score_last_90_days > 8 THEN 1 ELSE 0 END AS quarterly_promoters,
        CASE WHEN score_last_90_days < 7 THEN 1 ELSE 0 END AS quarterly_detractors,
        CASE WHEN score_last_90_days > 6 AND score_last_90_days < 9 THEN 1 ELSE 0 END AS quarterly_passives,
        CASE WHEN score_last_90_days IS NOT NULL THEN 1 ELSE 0 END AS quarterly_nps_users
    FROM 
        spined sp
    LEFT JOIN 
        {{ ref('int_nps_score') }} nps_score
    ON 
        sp.server_id = nps_score.server_id 
        AND sp.user_id = nps_score.user_id 
        AND sp.activity_date = nps_score.event_date
)
SELECT * 
FROM 
    score_cte 
ORDER BY 
    activity_date DESC
