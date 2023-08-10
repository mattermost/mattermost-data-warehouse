{{
    config({
        "materialized": "incremental",
        "snowflake_warehouse": "transform_l"
    })
}}
with server_activity_dates as (
    select
        server_id, server_date
    from
        {{ ref('int_activity_latest_daily') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
        and server_id not in (select server_id from {{ ref('int_excludable_servers') }})

    union all

    select
        server_id, server_date
    from
        {{ ref('int_activity_legacy_latest_daily') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
        and server_id not in (select server_id from {{ ref('int_excludable_servers') }})
), server_date_range as (
    select
        server_id,
        min(server_date) as first_active_day,
        max(server_date) as last_active_day
    from server_activity_dates
    group by 1
), spined as (
    -- Use date spine to fill in missing days
    select
        sdr.server_id,
        all_days.date_day::date as activity_date,
        {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
    from
        server_date_range sdr
        left join {{ ref('telemetry_days') }} all_days
            on all_days.date_day >= sdr.first_active_day and all_days.date_day <= sdr.last_active_day
)
select
    s.daily_server_id,
    s.server_id,
    s.activity_date,
    coalesce(a.daily_active_users, l.daily_active_users, 0) as daily_active_users,
    coalesce(a.monthly_active_user, l.monthly_active_user, 0) as monthly_active_user,
    coalesce(a.count_registered_users, l.count_registered_users, 0) as count_registered_users,
    coalesce(a.registered_deactivated_users, l.registered_deactivated_users, 0) as registered_deactivated_users,
    a.daily_server_id is null and l.daily_server_id is null as is_missing_activity_data
from
    spined s
    -- Activity data (rudderstack)
    left join {{ ref('int_activity_latest_daily') }} a on s.daily_server_id = a.daily_server_id
    --  Activity data (segment)
    left join {{ ref('int_activity_legacy_latest_daily') }} l on s.daily_server_id = l.daily_server_id