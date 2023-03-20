WITH projects AS (
    SELECT
        *
    FROM
        {{ source('mattermost_jira', 'projects') }}
)
SELECT
    id,
    key,
    name
FROM
    projects