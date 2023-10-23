{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}
with activity_date_ranges as (
    -- Front-end telemetry based on user actions
    select
        server_id,
        min(activity_date) as start_date,
        max(activity_date) as end_date
    from
        {{ ref('int_user_active_days_spined') }}
    group by server_id

    union all

    -- Server-side telemetry for older versions of mattermost servers
    select
        server_id,
        min(server_date) as start_date,
        max(server_date) as end_date
    from
        {{ ref('int_server_telemetry_legacy_latest_daily') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
    group by
        server_id

    union all

    -- Server-side telemetry for newer versions of mattermost servers
    select
        server_id,
        min(server_date) as start_date,
        max(server_date) as end_date
    from
        {{ ref('int_server_telemetry_latest_daily') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
        -- and server_id not in (select server_id from {{ ref('int_excludable_servers') }} where server_id is not null)
    group by
        server_id

    union all

    -- Security update check
    select
        server_id,
        min(server_date) as start_date,
        max(server_date) as end_date
    from
        {{ ref('int_server_security_update_latest_daily') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
        -- and server_id not in (select server_id from {{ ref('int_excludable_servers') }} where server_id is not null)
    group by
        server_id
), agg_activity_dates as (
    -- Find min and max date range across sources
    select
        server_id,
        min(start_date) as first_active_day,
        max(end_date) as last_active_day
    from
        activity_date_ranges
    group by
        server_id
)
-- Use date spine to fill in missing days
select
    aad.server_id,
    all_days.date_day::date as activity_date,
    {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
from
    agg_activity_dates aad
    left join {{ ref('telemetry_days') }} all_days
        on all_days.date_day >= aad.first_active_day and all_days.date_day <= aad.last_active_day