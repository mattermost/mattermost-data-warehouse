-- Check if there are documented actions that do not have matching events
-- via telemetry.

with telemetry_actions as (
    select
        event_name, event_action, max(timestamp)
    from
        {{ ref('stg_incident_response_prod__tracks') }}
    where
        user_id is not null
    group by event_name, event_action
)
select
    pe.event_name, pe.event_action
from
    {{ ref('playbook_events') }} pe
    left join telemetry_actions t on t.event_name = pe.event_name and (t.event_action = pe.event_action or pe.event_action is null)
where
    t.event_name is null
    or t.event_action is null