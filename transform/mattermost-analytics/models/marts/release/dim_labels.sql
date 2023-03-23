SELECT
     issue_id,
     value::string as label
FROM
    {{ ref('stg_mattermost_jira__issues') }},
    LATERAL FLATTEN(INPUT => labels)