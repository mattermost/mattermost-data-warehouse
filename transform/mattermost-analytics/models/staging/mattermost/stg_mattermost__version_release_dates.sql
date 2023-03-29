WITH rd AS (
    SELECT
        *
    FROM
        {{ source('mattermost', 'version_release_date') }}
)
SELECT
    version AS version,
    'v' || REGEXP(version, '^\\d+\\.\\d') AS short_version,
    release_date::DATE AS planned_release_date,
    supported::BOOLEAN AS is_supported,
    release_number::INT AS release_number
FROM
    rd