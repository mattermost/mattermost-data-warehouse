select
    -- Identifiers
    daily_server_id,
    server_id,
    activity_date,
    installation_id,

    -- Dimensions
    {{ dbt_utils.generate_surrogate_key(['version_full']) }} AS version_id,
    installation_type,  -- Degenerate dimensions

    -- Facts
    is_enterprise_ready,
    is_cloud,
    count_reported_versions,

    -- Metadata
    has_telemetry_data,
    has_legacy_telemetry_data,
    has_diagnostics_data,
    is_missing_activity_data
from
    {{ ref('int_server_active_days_spined') }}