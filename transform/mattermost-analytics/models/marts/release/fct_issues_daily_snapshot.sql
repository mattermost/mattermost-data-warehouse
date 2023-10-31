select
    issue_id,
    issue_key,
    project_id,
    created_at,
    case
        when status_name in ('Closed', 'Done') then updated_at
    end as closed_at,
    issue_type_name as issue_type,
    status_name as status,
    resolution_name as resolution,
    {{ datediff("created_at", "closed_at", "day") }} as lead_time_in_days
from
    {{ ref('stg_mattermost_jira__issues') }}
