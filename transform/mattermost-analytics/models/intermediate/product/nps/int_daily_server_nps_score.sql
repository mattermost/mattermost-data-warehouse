WITH base_cte AS (
    SELECT
        event_date,
        server_id,
        server_version,
        user_role,
        COUNT(DISTINCT CASE WHEN nps_score.score > 8 THEN nps_score.user_id ELSE NULL END) AS promoters,
        COUNT(DISTINCT CASE WHEN nps_score.score < 7 THEN nps_score.user_id ELSE NULL END) AS detractors,
        COUNT(DISTINCT CASE WHEN nps_score.score > 6 AND nps_score.score < 9 THEN nps_score.user_id ELSE NULL END) AS passives,
        COUNT(DISTINCT user_id) AS nps_users
    FROM
          {{ ref('int_nps_score') }} nps_score
    GROUP BY
        event_date,
        server_id,
        server_version,
        user_role
),
tmp AS (
    SELECT
        td.date_day::DATE AS activity_date,
        server_id,
        user_role,
        CASE WHEN td.date_day::DATE = event_date THEN promoters ELSE 0 END AS promoters,
        CASE WHEN td.date_day::DATE = event_date THEN detractors ELSE 0 END AS detractors,
        CASE WHEN td.date_day::DATE = event_date THEN passives ELSE 0 END AS passives,
        CASE WHEN td.date_day::DATE = event_date THEN nps_users ELSE 0 END AS nps_users,
        MAX(server_version) AS server_version
    FROM
        {{ ref('telemetry_days') }} td
    LEFT JOIN
        base_cte b ON td.date_day::DATE >= event_date
    GROUP BY
        td.date_day::DATE,
        server_id,
        user_role,
        CASE WHEN td.date_day::DATE = event_date THEN promoters ELSE 0 END,
        CASE WHEN td.date_day::DATE = event_date THEN detractors ELSE 0 END,
        CASE WHEN td.date_day::DATE = event_date THEN passives ELSE 0 END,
        CASE WHEN td.date_day::DATE = event_date THEN nps_users ELSE 0 END
    HAVING
        td.date_day::DATE >= MIN(event_date)
)
SELECT
    *,
    SUM(promoters) OVER (PARTITION BY server_id, user_role, server_version ORDER BY activity_date DESC ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING) AS quarterly_promoters,
    SUM(detractors) OVER (PARTITION BY server_id, user_role, server_version ORDER BY activity_date DESC ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING) AS quarterly_detractors,
    SUM(passives) OVER (PARTITION BY server_id, user_role, server_version ORDER BY activity_date DESC ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING) AS quarterly_passives,
    SUM(nps_users) OVER (PARTITION BY server_id, user_role, server_version ORDER BY activity_date DESC ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING) AS quarterly_nps_users
FROM
    tmp
GROUP BY
    activity_date,
    server_id,
    user_role,
    promoters,
    detractors,
    passives,
    nps_users,
    server_version
ORDER BY
    activity_date DESC
