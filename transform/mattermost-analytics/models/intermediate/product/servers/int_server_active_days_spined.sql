{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}
with server_first_day_per_telemetry as (
    select
        server_id,
        min(server_date) as first_server_date,
        max(server_date) as last_server_date
    from
        {{ ref('int_server_telemetry_legacy_latest_daily') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
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
    group by
        server_id
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
        {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id,
        datediff(day, sadr.first_active_day, all_days.date_day::date) as age_in_days
    from
        server_activity_date_range sadr
        left join {{ ref('telemetry_days') }} all_days
            on all_days.date_day >= sadr.first_active_day and all_days.date_day <= sadr.last_active_day
)
select
    s.daily_server_id,
    s.server_id,
    s.activity_date,

    -- Server information
    coalesce(t.version_full, l.version_full, d.version_full) as version_full,
    coalesce(t.version_major, l.version_major, d.version_major) as version_major,
    coalesce(t.version_minor, l.version_minor, d.version_minor) as version_minor,
    coalesce(t.version_patch, l.version_patch, d.version_patch) as version_patch,
    coalesce(t.operating_system, l.operating_system, d.operating_system) as operating_system,
    coalesce(t.database_type, l.database_type, d.database_type) as database_type,
    coalesce(t.database_version, l.database_version) as _database_version,
    _database_version as database_version,
    case
        when regexp_like(_database_version, '\\d+\.\\d+.\\d+.*') then regexp_substr(_database_version, '\\d+\.\\d+')
        when regexp_like(_database_version, '\\d+\.\\d+.*') then regexp_substr(_database_version, '\\d+\.\\d+')
        when regexp_like(_database_version, '\\d+beta.*') then regexp_substr(_database_version, '\\d+') || '.0'
        when regexp_like(_database_version, '\\d+devel.*') then regexp_substr(_database_version, '\\d+') || '.0'
        when regexp_like(_database_version, '\\d+rc.*') then regexp_substr(_database_version, '\\d+') || '.0'
    end as database_version_semver,
    split_part(database_version_semver, '.', 1)::int as database_version_major,
    split_part(database_version_semver, '.', 2)::int as database_version_minor,
    coalesce(t.edition, l.edition, d.is_enterprise_ready) as is_enterprise_ready,
    case
        when coalesce(t.edition, l.edition, d.is_enterprise_ready) = true then 'E0'
        when coalesce(t.edition, l.edition, d.is_enterprise_ready) = false then 'TE'
        else 'Unknown'
    end as binary_edition,
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

    -- Activity data
    coalesce(activity.daily_active_users, legacy_activity.daily_active_users, d.count_active_users, 0) as daily_active_users,
    coalesce(activity.monthly_active_users, legacy_activity.monthly_active_users, 0) as monthly_active_users,
    coalesce(activity.count_registered_users, legacy_activity.count_registered_users, d.count_users,  0) as count_registered_users,
    coalesce(activity.count_registered_deactivated_users, legacy_activity.count_registered_deactivated_users, 0) as count_registered_deactivated_users,
    coalesce(activity.count_registered_users, legacy_activity.count_registered_users) - coalesce(activity.count_registered_deactivated_users, legacy_activity.count_registered_deactivated_users) as count_registered_active_users,

    coalesce(activity.count_public_channels, legacy_activity.count_public_channels, 0) as count_public_channels,
    coalesce(activity.count_private_channels, legacy_activity.count_private_channels, 0) as count_private_channels,
    coalesce(activity.count_teams, legacy_activity.count_teams, d.count_teams, 0) as count_teams,
    coalesce(activity.count_slash_commands, legacy_activity.count_slash_commands, 0) as count_slash_commands,
    coalesce(activity.count_direct_message_channels, legacy_activity.count_direct_message_channels, 0) as count_direct_message_channels,
    coalesce(activity.count_posts, legacy_activity.count_posts, 0) as count_posts,


    -- Server lifecycle information
    s.age_in_days,

    -- Metadata regarding telemetry/activity availability
    t.daily_server_id is not null as has_telemetry_data,
    l.daily_server_id is not null as has_legacy_telemetry_data,
    d.daily_server_id is not null as has_diagnostics_data,
    activity.daily_server_id is null and legacy_activity.daily_server_id is null as is_missing_activity_data

from
    spined s
    -- Telemetry (rudderstack) data
    left join {{ ref('int_server_telemetry_latest_daily') }} t on s.daily_server_id = t.daily_server_id
    -- Telemetry (segment) data
    left join {{ ref('int_server_telemetry_legacy_latest_daily') }} l on s.daily_server_id = l.daily_server_id
    -- Security update logs (diagnostics) data
    left join {{ ref('int_server_security_update_latest_daily') }} d on s.daily_server_id = d.daily_server_id
    -- Activity data (rudderstack)
    left join {{ ref('int_activity_latest_daily') }} activity on s.daily_server_id = activity.daily_server_id
    --  Activity data (segment)
    left join {{ ref('int_activity_legacy_latest_daily') }} legacy_activity on s.daily_server_id = legacy_activity.daily_server_id

where
    s.server_id is not null
