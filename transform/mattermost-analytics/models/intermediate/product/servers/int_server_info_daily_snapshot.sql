{{
    config({
        "materialized": "table",
    })
}}
select
    coalesce(s.id, l.id) as daily_server_id,
    coalesce(s.id, l.server_id) as server_id,
    coalesce(s.server_date, l.server_date) as snapshot_date,
    coalesce(s.version_full, l.version_full) as version_full,
    coalesce(s.version_major, l.version_major) as version_major,
    coalesce(s.version_minor, l.version_minor) as version_minor,
    coalesce(s.version_patch, l.version_patch) as version_patch,
    coalesce(s.operating_system, l.operating_system) as operating_system,
    coalesce(s.database_type, l.database_type) as database_type,
    coalesce(s.database_version, l.database_version) as database_version,
    coalesce(s.edition, l.edition) as edition,
    s.installation_id,
    s.server_ip,
    s.installation_type,
    array_distinct(
        array_cat(
                coalesce(s.reported_versions, array_construct()),
                coalesce(l.reported_versions, array_construct())
        )
    ) as reported_versions,
    array_size(array_distinct(
        array_cat(
                coalesce(s.reported_versions, array_construct()),
                coalesce(l.reported_versions, array_construct())
        )
    )) as count_reported_versions
from
    {{ ref('int_server_telemetry_legacy_latest_daily') }} l
    full outer join {{ ref('int_server_telemetry_latest_daily') }} s on s.id = l.id
