select
    -- Identifiers
    s.daily_server_id,
    s.server_id,
    s.activity_date,

    -- Dimensions
    {{ dbt_utils.generate_surrogate_key(['version_full']) }} AS version_id,
    -- Degenerate dimensions
    s.installation_type,
    case
        when s.count_registered_active_users < 10 then '< 10'
        when s.count_registered_active_users >= 10 and s.count_registered_active_users < 100 then '10-100'
        when s.count_registered_active_users >= 100 and s.count_registered_active_users < 250 then '100-250'
        when s.count_registered_active_users >= 250 and s.count_registered_active_users < 500 then '250-500'
        when s.count_registered_active_users >= 500 and s.count_registered_active_users < 1000 then '500-1000'
        when s.count_registered_active_users >= 1000 and s.count_registered_active_users < 2500 then '1000-2500'
        when s.count_registered_active_users >= 2500 then '>=2500'
        else 'Unknown'
    end as registered_user_bin,

    -- Facts
    s.daily_active_users,
    s.count_registered_active_users,
    s.is_enterprise_ready,
    s.count_reported_versions,
    -- TODO: handle cloud instances.

    -- Metadata
    s.has_telemetry_data,
    s.has_legacy_telemetry_data,
    s.has_diagnostics_data,
    s.is_missing_activity_data,
    l.daily_server_id is null as is_missing_license_data
from
    {{ ref('int_server_active_days_spined') }} s
    left join {{ ref('int_server_license_daily') }} l on s.daily_server_id = l.daily_server_id