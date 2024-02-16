SELECT
    a.activity_date,
    a.server_id AS server_id,
    a.user_role AS user_role,
    b.server_version AS server_version,
    SUM(a.promoters) AS promoters,
    SUM(a.detractors) AS detractors,
    SUM(a.passives) AS passives,
    SUM(a.nps_users) AS nps_users,
    SUM(a.quarterly_promoters) AS quarterly_promoters,
    SUM(a.quarterly_detractors) AS quarterly_detractors,
    SUM(a.quarterly_passives) AS quarterly_passives,
    SUM(a.quarterly_nps_users) AS quarterly_nps_users
FROM
    {{ ref('int_user_nps_score_spined') }} a 
    join {{ ref('int_nps_server_version_spined') }} b 
    on a.server_id = b.server_id and a.activity_date = b.activity_date
GROUP BY
    a.activity_date,
    a.server_id,
    a.user_role,
    b.server_version
ORDER BY
    a.activity_date DESC
