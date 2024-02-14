SELECT
    activity_date,
    server_id AS server_id,
    user_role AS user_role,
    server_version AS server_version,
    SUM(promoters) AS promoters,
    SUM(detractors) AS detractors,
    SUM(passives) AS passives,
    SUM(nps_users) AS nps_users,
    SUM(quarterly_promoters) AS quarterly_promoters,
    SUM(quarterly_detractors) AS quarterly_detractors,
    SUM(quarterly_passives) AS quarterly_passives,
    SUM(quarterly_nps_users) AS quarterly_nps_users
FROM
    {{ ref('int_user_nps_score_spined') }} b
GROUP BY
    activity_date,
    server_id,
    user_role,
    server_version
ORDER BY
    activity_date DESC
