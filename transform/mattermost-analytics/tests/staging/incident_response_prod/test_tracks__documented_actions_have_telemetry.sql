-- Check if there are documented actions that do not have matching events
-- via telemetry.

with telemetry_actions as (
    select
        event_name, event_action, max(timestamp)
    from
        {{ ref('stg_incident_response_prod__tracks') }}
    group by event_name, event_action
)
select
    pe.event_name, pe.event_action
from
    {{ ref('playbooks_tracking_plan') }} pe
    left join telemetry_actions t
        on t.event_name = pe.event_name
            -- Handle nulls in event_action
            and ((t.event_action = pe.event_action) or (t.event_action is null and pe.event_action is null))
where
    t.event_name is null
    and t.event_action is null