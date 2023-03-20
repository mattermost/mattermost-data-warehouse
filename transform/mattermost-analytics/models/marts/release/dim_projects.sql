SELECT
    {{ dbt_utils.star(ref('stg_mattermost_jira__projects')) }}
FROM
    ref('stg_mattermost_jira__projects')