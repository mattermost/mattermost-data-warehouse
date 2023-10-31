select
    {{ dbt_utils.star(ref('stg_mattermost_jira__projects')) }}
from
    {{ ref('stg_mattermost_jira__projects') }}