select
     issue_id,
     value::string as label
from
    {{ ref('stg_mattermost_jira__issues') }},
    lateral FLATTEN(INPUT => labels)