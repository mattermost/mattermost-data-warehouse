{{config({
    "materialized": "table",
    "snowflake_warehouse": "transform_l",
  })
}}

with server_side_activity as (
    select
        distinct server_id, server_date
    from
        {{ ref('int_server_telemetry_legacy_latest_daily') }}
    union all
    select
        distinct server_id, server_date
    from
        {{ ref('int_server_telemetry_latest_daily') }}
),
server_summary as (
    -- Servers side telemetry summary
    select
        server_id,
        count(distinct server_date) as count_server_active_days,
        min(server_date) as first_date,
        max(server_date) as last_date
    from
        server_side_activity
    group by 1
),
user_activity as (
    select
        distinct server_id, activity_date
    from
        {{ ref('int_user_active_days_legacy_telemetry') }}
    union all
    select
        distinct server_id, activity_date
    from
        {{ ref('int_user_active_days_server_telemetry') }}
),
user_summary as (
    -- User telemetry summary
    select
        server_id,
        count(distinct activity_date) as count_user_active_days,
        min(activity_date) as first_date,
        max(activity_date) as last_date
    from
        user_activity
    group by 1
)
select
    coalesce(server_summary.server_id, user_summary.server_id) as server_id,
    case
        when coalesce(count_server_active_days, 0) = 1 and coalesce(count_user_active_days, 0) = 0 then 'Single day server-side telemetry only'
        when coalesce(count_server_active_days, 0) = 0 and coalesce(count_user_active_days, 0) = 1 then 'Single day user telemetry only'
        -- Should we also compare dates?
        when coalesce(count_server_active_days, 0) = 1 and coalesce(count_user_active_days, 0) = 1 then 'Single day telemetry only'
        else null
    end as reason
from
    server_summary
    full outer join user_summary on server_summary.server_id = user_summary.server_id
where
    reason is not null