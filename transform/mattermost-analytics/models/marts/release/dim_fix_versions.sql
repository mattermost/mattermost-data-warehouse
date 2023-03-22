
WITH unparsed_fix_versions AS (
    -- Implement filtering on this layer as it's only used here.
    SELECT
        issue_key,
        value:name::string as fix_version_name,
        REGEXP_LIKE(fix_version_name, '.*v\\d+\.\\d+.*', 'i') AS target_version,
        CASE
            WHEN fix_version_name ILIKE '%mobile%' THEN 'Mobile'
            WHEN fix_version_name ILIKE '%desktop%' THEN 'Desktop'
            WHEN fix_version_name ILIKE '%mobile%' THEN 'Mobile'
            WHEN fix_version_name ILIKE '%playbooks%' THEN 'Playbooks'
            WHEN fix_version_name ILIKE '%ir%' THEN 'IR'
            WHEN fix_version_name ILIKE '%cloud%' THEN 'Cloud'
            WHEN fix_version_name ILIKE '%apps%' THEN 'Apps'
            ELSE NULL
        END AS type,
        REGEXP_SUBSTR(fix_version_name, '\\d{2}/\\d{2}/\\d{2}') AS cloud_release_date
    FROM
        {{ ref('stg_mattermost_jira__issues') }},
        LATERAL FLATTEN(INPUT => fix_versions)
    WHERE
        -- Keep only relevant fix versions - ones that contain a version in the form `v[major].[minor]`
        REGEXP_LIKE(fix_version_name, '.*v\\d+\.\\d+.*', 'i')
)
SELECT
    issue_key,
    fix_version_name AS fix_version,
    -- Do the parsing
    CASE WHEN
FROM
    unparsed_fix_versions
