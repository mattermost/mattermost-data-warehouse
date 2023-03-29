
WITH unparsed_fix_versions AS (
    -- Implement filtering on this layer as it's only used here.
    SELECT
        issue_id,
        value:name::string as fix_version_name,
        -- Break down different variations of target version
        REGEXP_SUBSTR(fix_version_name, 'v\\d+\.\\d+') AS semver,
        REGEXP_SUBSTR(semver, 'v(\\d+)', 1, 1, 'e', 1)::int AS version_major,
        REGEXP_SUBSTR(semver, '\\.(\\d+)', 1, 1, 'e', 1)::int AS version_minor,
        CASE
            WHEN fix_version_name ILIKE '%mobile%' THEN 'Mobile'
            WHEN fix_version_name ILIKE '%desktop%' THEN 'Desktop'
            WHEN fix_version_name ILIKE '%playbooks%' THEN 'Playbooks'
            WHEN fix_version_name ILIKE '%ir%' THEN 'IR'
            WHEN fix_version_name ILIKE '%cloud%' THEN 'Cloud'
            WHEN fix_version_name ILIKE '%apps%' THEN 'Apps'
        END AS component,
        TO_DATE(REGEXP_SUBSTR(fix_version_name, '\\d{2}/\\d{2}/\\d{2}'), 'mm/dd/yy') AS cloud_release_date
    FROM
        {{ ref('stg_mattermost_jira__issues') }},
        LATERAL FLATTEN(INPUT => fix_versions)
    WHERE
        -- Keep only relevant fix versions - ones that contain a version in the form `v[major].[minor]`
        REGEXP_LIKE(fix_version_name, '.*v\\d+\.\\d+.*', 'i')
)
SELECT
    fv.issue_id,
    fv.fix_version_name AS fix_version,
    fv.semver,
    fv.version_major,
    fv.version_minor,
    fv.component,
    fv.cloud_release_date,
    rd.planned_release_date
FROM
    unparsed_fix_versions fv
    -- Add planned release date by looking up dates ONLY for Cloud and On Prem releases)
    LEFT JOIN {{ ref('stg_mattermost__version_release_dates') }} rd
        ON fv.semver = rd.short_version AND (fv.component IS NULL OR fv.component = 'Cloud')
