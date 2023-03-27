WITH projects AS (
    SELECT
        *
    FROM
        {{ source('mattermost_jira', 'projects') }}
)
SELECT
    id AS project_id,
    key AS project_key,
    name
FROM
    projects