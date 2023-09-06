select
    -- Identifiers
    daily_server_id,
    server_id,
    activity_date,

    -- Dimensions
    {{ dbt_utils.generate_surrogate_key(['version_full']) }} AS version_id,
    -- Degenerate dimensions
    installation_type,
    case
        when count_registered_active_users < 10 then '< 10'
        when count_registered_active_users >= 10 and count_registered_active_users < 100 then '10-100'
        when count_registered_active_users >= 100 and count_registered_active_users < 250 then '100-250'
        when count_registered_active_users >= 250 and count_registered_active_users < 1000 then '250-1000'
        when count_registered_active_users >= 1000 and count_registered_active_users < 2500 then '1000-2500'
        when count_registered_active_users >= 2500 then '>=2500'
        else 'Unknown'
    end as registered_user_bin,

    -- Facts
    is_enterprise_ready,
    count_reported_versions,

    -- Metadata
    has_telemetry_data,
    has_legacy_telemetry_data,
    has_diagnostics_data,
    is_missing_activity_data
from
    {{ ref('int_server_active_days_spined') }}