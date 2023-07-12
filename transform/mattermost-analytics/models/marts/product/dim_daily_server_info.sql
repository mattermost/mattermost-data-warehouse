{{
    config({
        "snowflake_warehouse": "transform_l"
    })
}}
select
    daily_server_id,
    server_id,
    activity_date,
    version_full,
    version_major,
    version_minor,
    version_patch,
    operating_system,
    database_type,
    database_version,
    is_enterprise_ready,
    installation_id,
    is_cloud,
    server_ip,
    installation_type,
    count_reported_versions,
    has_telemetry_data,
    has_legacy_telemetry_data,
    has_diagnostics_data
from
    {{ ref('int_server_active_days_spined') }}
where
    server_id not in (
        select server_id from {{ ref('int_excludable_servers') }}
    )

