    SELECT
        event_date as event_date,
        server_id as server_id,
        user_id as user_id,
        server_version as server_version,
        score as score
    FROM
        {{ ref('int_nps_score') }} nps_score
    qualify row_number() over (partition by server_id, user_id, server_version order by event_date desc) = 1
