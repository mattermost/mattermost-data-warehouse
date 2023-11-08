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
    is_enterprise_ready,
    case
        when is_enterprise_ready = true then 'E0'
        when is_enterprise_ready = false then 'TE'
        else 'Unknown'
    end as binary_edition,
    installation_id,
    server_ip,
    installation_type,
    count_reported_versions,
    has_telemetry_data,
    has_legacy_telemetry_data,
    has_diagnostics_data
from
    {{ ref('int_server_active_days_spined') }}
