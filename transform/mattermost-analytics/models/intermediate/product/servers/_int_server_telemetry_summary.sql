with user_telemetry_servers as (
    select
        server_id,
        count(distinct activity_date) as count_user_active_days
    from
        {{ ref('int_user_active_days_spined') }}
    where
        is_active_today
    group by server_id
),
server_only_telemetry as (
    -- Servers with only server side telemetry
    select
        server_id,
        count(distinct activity_date) as count_server_active_days
    from
        {{ ref('int_server_active_days_spined') }}
    where
        has_telemetry_data or has_legacy_telemetry_data
    group by server_id
)
select
    coalesce(uts.server_id, sot.server_id) as server_id,
    coalesce(uts.count_user_active_days, 0) as count_user_active_days,
    coalesce(sot.count_server_active_days, 0) as count_server_active_days
from
    user_telemetry_servers uts
    full outer join server_only_telemetry sot on uts.server_id = sot.server_id