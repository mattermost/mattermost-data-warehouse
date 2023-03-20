SELECT
    issue_id,
    issue_key,
    project_id,
    created_at,
    CASE
        WHEN status_name IN ('Closed', 'Done') THEN updated_at
        ELSE NULL
    END AS closed_at,
    status_name AS status,
    resolution_description AS resolution,
    {{ datediff("created_at", "closed_at", "day") }} AS lead_time_in_days
FROM
    {{ ref('stg_mattermost_jira__issues') }}
