{{
    config({
        "materialized": "table",
        "unique_key": ['daily_user_id'],
        "cluster_by": ['activity_date', 'server_id']
    })
}}

with server_first_day_per_telemetry as (
    select
        server_id,
        min(activity_date) as first_active_day,
        max(activity_date) as last_active_date
    from
        {{ ref('int_boards_client_active_days') }}
    where
        activity_date >= '{{ var('telemetry_start_date')}}'
    group by 1

    union all

    select
        server_id,
        min(server_date) as first_active_day,
        max(server_date) as last_active_date
    from
        {{ ref('int_boards_server_active_days') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
    group by 1

), server_activity_date_range as (
    select
        server_id,
        min(first_server_date) as first_active_day,
        max(last_server_date) as last_active_day
    from
        server_first_day_per_telemetry
    group by
        server_id
), spined as (
    -- Use date spine to fill in missing days
    select
        sadr.server_id,
        all_days.date_day::date as activity_date,
        {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
    from
        server_activity_date_range sadr
        left join {{ ref('telemetry_days') }} all_days
            on all_days.date_day >= sadr.first_active_day and all_days.date_day <= sadr.last_active_day
), user_activity_daily as (
    -- Aggregate client telemetry per day to bring to same granularity as server reported telemetry
    select
        {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
        , activity_date
        , server_id
        , sum(is_active_today::integer) as daily_active_users
        , sum(is_active_last_7_days::integer) as weekly_active_users
        , sum(is_active_last_30_days::integer) as monthly_active_users
    from {{ ref('int_boards_client_active_days') }}
    group by activity_date, server_id
)
select
    s.daily_server_id,
    s.server_id,
    s.activity_date,

    -- Telemetry information
    t.daily_active_users,
    t.weekly_active_users,
    t.monthly_active_users,

    -- Server activity information
    a.daily_active_users as server_daily_active_users,
    a.weekly_active_users as server_weekly_active_users,
    a.monthly_active_users as server_monthly_active_users,
    a.count_registered_users as count_registered_users,

    -- Metadata regarding telemetry/activity availability
    t.daily_server_id is not null as has_client_data,
    a.daily_server_id is not null as has_server_data
from
    spined s
    left join user_activity_daily t on s.daily_server_id = t.daily_server_id
    left join {{ ref('int_boards_server_active_days') }} a on s.daily_server_id = a.daily_server_id
where
    s.server_id is not null