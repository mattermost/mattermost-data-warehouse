{{
    config({
        "materialized": "table",
    })
}}

WITH first_score_day AS (
    SELECT 
        server_id,
        user_id,
        MIN(event_date) AS min_event_date
    FROM 
        {{ ref('int_nps_score') }}
    WHERE event_date >= '{{ var('nps_start_date')}}'
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
        {{ ref('nps_days') }} td 
    ON 
        date_day::date >= min_event_date
), score_cte AS (
    SELECT 
        sp.activity_date,
        sp.server_id,
        sp.user_id,
        nps_score.score,
         FIRST_VALUE(nps_score.user_role IGNORE NULLS) OVER (
            PARTITION BY sp.server_id, sp.user_id 
            ORDER BY activity_date DESC
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS user_role,
        CASE WHEN nps_score.score > 8 THEN 1 ELSE 0 END AS promoters,
        CASE WHEN nps_score.score < 7 THEN 1 ELSE 0 END AS detractors,
        CASE WHEN nps_score.score > 6 AND nps_score.score < 9 THEN 1 ELSE 0 END AS passives,
        CASE WHEN nps_score.score IS NOT NULL THEN 1 ELSE 0 END AS nps_users,
        FIRST_VALUE(score IGNORE NULLS) OVER (
            PARTITION BY sp.server_id, sp.user_id 
            ORDER BY activity_date DESC
            ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING
        ) AS score_last_90d,
        CASE WHEN score_last_90d > 8 THEN 1 ELSE 0 END AS promoters_last90d,
        CASE WHEN score_last_90d < 7 THEN 1 ELSE 0 END AS detractors_last90d,
        CASE WHEN score_last_90d > 6 AND score_last_90d < 9 THEN 1 ELSE 0 END AS passives_last90d,
        CASE WHEN score_last_90d IS NOT NULL THEN 1 ELSE 0 END AS nps_users_last90d
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
