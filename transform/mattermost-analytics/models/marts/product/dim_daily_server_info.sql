{{
    config({
        "snowflake_warehouse": "transform_l"
    })
}}
select
    daily_server_id,
    server_id,
    activity_date,
    operating_system,
    database_type,
    database_version,
    database_version_semver,
    database_version_major,
    database_version_minor,
    is_enterprise_ready,
    binary_edition,
    installation_id,
    server_ip,
    installation_type,
    count_reported_versions,
    age_in_days,
    has_telemetry_data,
    has_legacy_telemetry_data,
    has_diagnostics_data
from
    {{ ref('int_server_active_days_spined') }}
