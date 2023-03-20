WITH projects AS (
    SELECT
        *
    FROM
        {{ source('mattermost', 'projects') }}
)
SELECT
    id,
    key,
    name
FROM
    projects