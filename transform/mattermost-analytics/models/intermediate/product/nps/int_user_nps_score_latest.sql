    SELECT
        event_date,
        server_id,
        user_id,
        server_version,
        nps_score
    FROM
        {{ ref('int_nps_score') }} nps_score
    qualify row_number() over (partition by server_id, user_id, server_version order by event_date desc) = 1
