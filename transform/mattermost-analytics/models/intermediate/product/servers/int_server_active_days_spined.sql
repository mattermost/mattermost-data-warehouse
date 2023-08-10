with server_first_day_per_telemetry as (
    select
        server_id,
        min(server_date) as first_server_date,
        max(server_date) as last_server_date
    from
        {{ ref('int_server_telemetry_legacy_latest_daily') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
        and server_id not in (select server_id from {{ ref('int_excludable_servers') }})
    group by
        server_id

    union all

    select
        server_id,
        min(server_date) as first_server_date,
        max(server_date) as last_server_date
    from
        {{ ref('int_server_telemetry_latest_daily') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
        and server_id not in (select server_id from {{ ref('int_excludable_servers') }})
    group by
        server_id

    union all

    select
        server_id,
        min(server_date) as first_server_date,
        max(server_date) as last_server_date
    from
        {{ ref('int_server_security_update_latest_daily') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
        and server_id not in (select server_id from {{ ref('int_excludable_servers') }})
    group by
        server_id
), server_first_active_day as (
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
        first_day.server_id,
        all_days.date_day::date as activity_date,
        {{ dbt_utils.generate_surrogate_key(['activity_date', 'server_id']) }} AS daily_server_id
    from
        server_first_active_day first_day
        left join {{ ref('telemetry_days') }} all_days
            on all_days.date_day >= first_day.first_active_day and all_days.date_day <= first_day.last_active_day
)
select
    s.daily_server_id,
    s.server_id,
    s.activity_date,
    coalesce(t.version_full, l.version_full, d.version_full) as version_full,
    coalesce(t.version_major, l.version_major, d.version_major) as version_major,
    coalesce(t.version_minor, l.version_minor, d.version_minor) as version_minor,
    coalesce(t.version_patch, l.version_patch, d.version_patch) as version_patch,
    coalesce(t.operating_system, l.operating_system, d.operating_system) as operating_system,
    coalesce(t.database_type, l.database_type, d.database_type) as database_type,
    coalesce(t.database_version, l.database_version) as database_version,
    coalesce(t.edition, l.edition, d.is_enterprise_ready) as is_enterprise_ready,
    t.installation_id,
    case
        when t.server_id is not null and t.installation_id is not null then true
        when t.server_id is not null and t.installation_id is null then false
        else null
    end as is_cloud,
    coalesce(t.server_ip, d.server_ip) as server_ip,
    t.installation_type,
    array_distinct(
        array_cat(
            coalesce(t.reported_versions, array_construct()),
            array_cat(
                coalesce(l.reported_versions, array_construct()),
                coalesce(d.reported_versions, array_construct())
            )
        )
    ) as reported_versions,
    array_size(array_distinct(
        array_cat(
            coalesce(t.reported_versions, array_construct()),
            array_cat(
                coalesce(l.reported_versions, array_construct()),
                coalesce(d.reported_versions, array_construct())
            )
        )
    )) as count_reported_versions,
    t.daily_server_id is not null as has_telemetry_data,
    l.daily_server_id is not null as has_legacy_telemetry_data,
    d.daily_server_id is not null as has_diagnostics_data
from
    spined s
    -- Telemetry (rudderstack) data
    left join {{ ref('int_server_telemetry_latest_daily') }} t on s.daily_server_id = t.daily_server_id
    -- Telemetry (segment) data
    left join {{ ref('int_server_telemetry_legacy_latest_daily') }} l on s.daily_server_id = l.daily_server_id
    -- Security update logs (diagnostics) data
    left join {{ ref('int_server_security_update_latest_daily') }} d on s.daily_server_id = d.daily_server_id
