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
server_side_activity as (
    select
        server_id, server_date
    from
        {{ ref('int_server_telemetry_legacy_latest_daily') }}
    union all
    select
        server_id, server_date
    from
        {{ ref('int_server_telemetry_latest_daily') }}
),
server_only_telemetry as (
    -- Servers with only server side telemetry
    select
        server_id, count(distinct server_date) as count_server_active_days
    from
        server_side_activity
    group by 1
)
select
    coalesce(uts.server_id, sot.server_id) as server_id,
    coalesce(uts.count_user_active_days, 0) as count_user_active_days,
    coalesce(sot.count_server_active_days, 0) as count_server_active_days
from
    user_telemetry_servers uts
    full outer join server_only_telemetry sot on uts.server_id = sot.server_id