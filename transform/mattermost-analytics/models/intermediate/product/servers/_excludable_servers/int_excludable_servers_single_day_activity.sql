select
    server_id,
    case
        -- Server side telemetry only
        when count_server_active_days = 1 and count_user_active_days = 0 then 'Single day server-side telemetry only'
        -- User telemetry only
        when count_server_active_days = 0 and count_user_active_days = 1 then 'Single day user telemetry only'
        -- Single day telemetry reported
        when count_server_active_days = 1 and count_server_active_days = 1 then 'Single day telemetry only'
        -- Security only reported
        when count_server_active_days = 0 and count_server_active_days = 0 and count_diagnostics_active_days = 1 then 'Single day security only'
        else null
    end as reason
from
    {{ ref('int_server_summary') }}
where
    reason is not null