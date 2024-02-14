WITH base_cte AS (
    SELECT
        activity_date,
        server_id,
        server_version,
        user_role,
        COUNT(DISTINCT CASE WHEN nps_score.score > 8 THEN nps_score.user_id ELSE NULL END) AS promoters,
        COUNT(DISTINCT CASE WHEN nps_score.score < 7 THEN nps_score.user_id ELSE NULL END) AS detractors,
        COUNT(DISTINCT CASE WHEN nps_score.score > 6 AND nps_score.score < 9 THEN nps_score.user_id ELSE NULL END) AS passives,
        COUNT(DISTINCT user_id) AS nps_users
    FROM
          {{ ref('int_user_nps_score_spined') }} nps_score
    GROUP BY
        event_date,
        server_id,
        server_version,
        user_role
)   SELECT
        activity_date,
        server_id AS server_id,
        user_role AS user_role,
        server_version AS server_version,
        SUM(promoters) AS promoters,
        SUM(detractors) AS detractors,
        SUM(passives) AS passives,
        SUM(nps_users) AS nps_users
    FROM
        base_cte b
    GROUP BY
        activity_date,
        server_id,
        user_role,
        server_version
ORDER BY
    activity_date DESC
