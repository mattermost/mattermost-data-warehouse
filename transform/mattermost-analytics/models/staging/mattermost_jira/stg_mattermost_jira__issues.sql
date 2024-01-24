WITH issues AS (
    SELECT
        *
    FROM
        {{ source('mattermost_jira', 'issues') }}
)
SELECT
    id AS issue_id,
    key AS issue_key,
    -- Parent
    fields:parent.id::INTEGER AS parent_id,
    fields:parent.key::VARCHAR AS parent_key,
    -- Project
    fields:project.id::integer AS project_id,
    fields:project.key::varchar AS project_key,
    -- Description
    fields:summary::VARCHAR AS summary,
    fields:description::VARCHAR AS description,
    -- Issue type
    fields:issuetype.id::INTEGER AS issue_type_id,
    fields:issuetype.name::VARCHAR AS issue_type_name,
    -- Reporter
    fields:reporter.accountId::VARCHAR AS reporter_account_id,
    fields:reporter.displayName::VARCHAR AS reporter_display_name,
    -- Creator
    fields:creator.accountId::VARCHAR AS creator_account_id,
    fields:creator.displayName::VARCHAR AS creator_display_name,
    -- Status
    fields:status.id::INTEGER AS status_id,
    fields:status.name::VARCHAR AS status_name,
    fields:status.statusCategory.id::INTEGER AS status_category_id,
    fields:status.statusCategory.name::VARCHAR AS status_category_name,
    fields:status.statusCategory.key::VARCHAR AS status_category_key,
    -- Resolution
    fields:resolution.id::VARCHAR AS resolution_id,
    fields:resolution.name::VARCHAR AS resolution_name,
    CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(fields:resolutiondate::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZHTZM')) AS resolution_date,
    -- Labels:
    fields:labels AS labels,
    -- Fix Version
    fields:fixVersions AS fix_versions,
    -- Custom fields
    fields:customfield_11145:value::varchar as environment,
    -- Timestamps
    CAST(fields:created AS {{ dbt.type_timestamp() }})  AS created_at,
    CAST(fields:updated AS {{ dbt.type_timestamp() }})  AS updated_at
FROM
    issues
