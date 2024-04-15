-- Check if there are actions sent via telemetry that are not in the list of known events/actions.
with telemetry_actions as (
    select
        event_name, event_action, count(*) as total_events
    from
        {{ ref('stg_incident_response_prod__tracks') }}
    group by event_name, event_action
)
select
    t.event_name, t.event_action
from
    telemetry_actions t
    left join {{ ref('playbook_events') }} pe
        on t.event_name = pe.event_name
            -- Handle nulls in event_action
            and ((t.event_action = pe.event_action) or (t.event_action is null and pe.event_action is null))
where
    pe.event_name is null
    and pe.event_action is null
    -- Heuristic to exclude low cardinality spam messages
    and t.total_events > 10
