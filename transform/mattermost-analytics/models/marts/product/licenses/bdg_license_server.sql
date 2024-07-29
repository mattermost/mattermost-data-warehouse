-- Bridge table to map license ID to server ID
select
    {{ dbt_utils.generate_surrogate_key(['license_id', 'server_id', 'installation_id']) }} as bdg_license_server_id
    , license_id
    , server_id
    , installation_id
    , min(license_telemetry_date) as first_telemetry_date
    , max(license_telemetry_date) as last_telemetry_date
from
    {{ ref('int_server_license_daily') }}
group by license_id, server_id, installation_id