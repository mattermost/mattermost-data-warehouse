SELECT
    issue_id,
    issue_key,
    project_id,
    created_at,
    CASE
        WHEN status_name IN ('Closed', 'Done') THEN updated_at
    END AS closed_at,
    issue_type_name AS issue_type,
    status_name AS status,
    resolution_name AS resolution,
    {{ datediff("created_at", "closed_at", "day") }} AS lead_time_in_days
FROM
    {{ ref('stg_mattermost_jira__issues') }}
