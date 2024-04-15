-- Check if there are actions sent via telemetry that are not in the list of known events/actions.
with telemetry_actions as (
    select
        event_name, event_action, count(*)
    from
        {{ ref('stg_incident_response_prod__tracks') }}
    where
        user_id is not null
    group by event_name, event_action
)
select
    t.event_name, t.event_action
from
    telemetry_actions t
        left join {{ ref('playbook_events') }} pe on t.event_name = pe.event_name and (t.event_action = pe.event_action or pe.event_action = '')
where
    pe.event_name is null
   or pe.event_action is null
