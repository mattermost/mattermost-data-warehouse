WITH issues AS (
    SELECT
        *
    FROM
        {{ source('mattermost', 'issues') }}
)
SELECT
    id AS issue_id,
    key,
    -- Parent
    fields:parent.id::INTEGER AS parent_id,
    fields:parent.key::VARCHAR AS parent_key,
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
    fields:resolution::VARCHAR AS resolution,
    CAST(fields:resolutiondate AS {{ dbt.type_timestamp() }}) AS resolution_date,
    -- Labels:
    fields:labels AS labels,
    -- Fix Version
    fields:fixVersions AS fix_versions,
    -- Timestamps
    CAST(fields:created AS {{ dbt.type_timestamp() }}) AS created_at,
    CAST(fields:updated AS {{ dbt.type_timestamp() }}) AS updated_at
FROM
    issues
